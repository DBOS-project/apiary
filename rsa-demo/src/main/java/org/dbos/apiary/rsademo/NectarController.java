package org.dbos.apiary.rsademo;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.rsademo.functions.NectarAddPost;
import org.dbos.apiary.rsademo.functions.NectarGetPosts;
import org.dbos.apiary.rsademo.functions.NectarLogin;
import org.dbos.apiary.rsademo.functions.NectarRegister;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.support.SessionStatus;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import org.springframework.web.servlet.view.RedirectView;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Controller
@SessionAttributes("logincredentials")
public class NectarController {
    private final ApiaryWorkerClient client;
    private final ApiaryWorker worker;

    public NectarController() throws SQLException {
        ApiaryConfig.captureUpdates = true;
        ApiaryConfig.captureReads = true;
        ApiaryConfig.recordInput = true;
        ApiaryConfig.captureMetadata = true;
        ApiaryConfig.provenancePort = 5433;  // Store provenance data in Vertica.

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos", ApiaryConfig.vertica, "localhost");

        this.worker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.vertica, "localhost");
        worker.registerConnection(ApiaryConfig.postgres, conn);
        worker.registerFunction("NectarRegister", ApiaryConfig.postgres, NectarRegister::new);
        worker.registerFunction("NectarLogin", ApiaryConfig.postgres, NectarLogin::new);
        worker.registerFunction("NectarAddPost", ApiaryConfig.postgres, NectarAddPost::new);
        worker.registerFunction("NectarGetPosts", ApiaryConfig.postgres, NectarGetPosts::new);
        worker.startServing();

        Thread rulesThread = new Thread(() -> {
            try {
                rulesThread();
            } catch (SQLException | InterruptedException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        });
        rulesThread.start();

        this.client = new ApiaryWorkerClient("localhost");
    }

    @GetMapping("/")
    public RedirectView index(Model model) {
        return new RedirectView("/home");
    }

    @GetMapping("/home")
    public String home(Model model) {
        return "home";
    }

    @GetMapping("/registration")
    public String registrationForm(Model model) {
        model.addAttribute("registration", new Credentials());
        return "registration";
    }

    @PostMapping("/registration")
    public String registrationSubmit(@ModelAttribute Credentials credentials, Model model) throws IOException {
        int success = client.executeFunction("NectarRegister", credentials.getUsername(), credentials.getPassword()).getInt();
        if (success != 0) {
            return "redirect:/home";
        }
        model.addAttribute("registration", credentials);
        return "registration_result";
    }

    @GetMapping("/login")
    public String loginForm(Model model) {
        model.addAttribute("login", new Credentials());
        return "login";
    }

    @PostMapping("/login")
    public RedirectView loginSubmit(@ModelAttribute Credentials credentials, @ModelAttribute("logincredentials") Credentials logincredentials, RedirectAttributes attributes) throws InvalidProtocolBufferException {
        int success = client.executeFunction("NectarLogin", credentials.getUsername(), credentials.getPassword()).getInt();
        if (success == 0) { // Login successful.
            logincredentials.setUsername(credentials.getUsername());
            logincredentials.setPassword(credentials.getPassword());
            // Ensure credentials are saved across page reloads.
            attributes.addFlashAttribute("logincredentials", logincredentials);
            return new RedirectView("/timeline");
        } else { // Login failed.
            return new RedirectView("/home");
        }
    }

    @RequestMapping("/logout")
    public String logoutSession(@ModelAttribute Credentials credentials, @ModelAttribute("logincredentials") Credentials logincredentials, Model model,
                                HttpServletRequest request,
                                SessionStatus sessionStatus) {
        sessionStatus.setComplete();
        return "redirect:/home";
    }

    private List<WebPost> findUserPosts(String username) throws InvalidProtocolBufferException {
        List<WebPost> postList = new ArrayList<>();
        String[] posts = client.executeFunction("NectarGetPosts", username).getStringArray();
        for (String post: posts) {
            WebPost webPost = new WebPost();
            JSONObject obj = (JSONObject) JSONValue.parse(post);
            webPost.setSender((String) obj.get("Sender"));
            webPost.setPostText((String) obj.get("PostText"));
            postList.add(webPost);
        }
        return postList;
    }

    @GetMapping("/timeline")
    public String timeline(Model model, @ModelAttribute("logincredentials") Credentials logincredentials) throws InvalidProtocolBufferException {
        if (logincredentials.getUsername() != null) {
            model.addAttribute("login", logincredentials);
            List<WebPost> postlist = findUserPosts(logincredentials.getUsername());
            model.addAttribute("timelinelist", postlist);
            model.addAttribute("addpost", new WebPost());
            return "timeline";
        } else {
            return "redirect:/home";
        }
    }

    @PostMapping("/timeline")
    public RedirectView timelinePostSubmit(@ModelAttribute WebPost webPost, @ModelAttribute("logincredentials") Credentials logincredentials) throws InvalidProtocolBufferException {
        if (logincredentials.getUsername() == null) {
            return new RedirectView("/home");
        }
        client.executeFunction("NectarAddPost", logincredentials.getUsername(), webPost.getReceiver(), webPost.getPostText());
        return new RedirectView("/timeline");
    }

    @ModelAttribute("logincredentials")
    public Credentials logincredentials() {
        return new Credentials();
    }

    String readPostsRule = "SELECT apiary_role, COUNT(*) AS num_invocations\n" +
            "FROM FuncInvocations\n" +
            "WHERE APIARY_TIMESTAMP / 1000000 >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '10 seconds'))\n" +
            "AND APIARY_PROCEDURENAME = 'NectarGetPosts'\n" +
            "GROUP BY APIARY_ROLE\n" +
            "ORDER BY APIARY_ROLE;";

    private void rulesThread() throws SQLException, InterruptedException, ClassNotFoundException {
        Class.forName("com.vertica.jdbc.Driver");
        Properties verticaProp = new Properties();
        verticaProp.put("user", "dbadmin");
        verticaProp.put("password", "password");
        verticaProp.put("loginTimeout", "35");
        verticaProp.put("streamingBatchInsert", "True");
        verticaProp.put("ConnectionLoadBalance", "1"); // Enable load balancing.
        Connection c = DriverManager.getConnection(
                String.format("jdbc:vertica://%s/apiary_provenance", "localhost"),
                verticaProp
        );
        c.setAutoCommit(true);

        PreparedStatement s = c.prepareStatement(readPostsRule);
        while (true) {
            ResultSet r = s.executeQuery();
            while (r.next()) {
                int numReads = r.getInt(2);
                if (numReads > 50) {
                    String badRole = r.getString(1);
                    this.worker.suspendRole(badRole);
                    System.out.printf("Suspicious activity: %s read %d different user accounts in 10 seconds\n", badRole, numReads);
                }
            }
            Thread.sleep(3000);
        }
    }
}
