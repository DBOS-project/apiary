package org.dbos.apiary.postgresdemo;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.support.SessionStatus;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import org.springframework.web.servlet.view.RedirectView;
import org.voltdb.client.ProcCallException;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Controller
@SessionAttributes("logincredentials")
public class NectarController {

    AtomicInteger execID = new AtomicInteger(0);
    AtomicInteger postID = new AtomicInteger(0);
    public static final int PKEY = 0;

    public NectarController() {
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
    public String registrationSubmit(@ModelAttribute Credentials credentials, Model model) throws InterruptedException, IOException, ProcCallException {
        model.addAttribute("registration", credentials);
        return "registration_result";
    }

    @GetMapping("/login")
    public String loginForm(Model model) {
        model.addAttribute("login", new Credentials());
        return "login";
    }

    @PostMapping("/login")
    public RedirectView loginSubmit(@ModelAttribute Credentials credentials, @ModelAttribute("logincredentials") Credentials logincredentials, RedirectAttributes attributes) throws InterruptedException, IOException, ProcCallException {
        boolean success = false;
        if (success) {
            logincredentials.setUsername(credentials.getUsername());
            logincredentials.setPassword(credentials.getPassword());
            // make sure the credential can be saved across page reload.
            attributes.addFlashAttribute("logincredentials", logincredentials);
            return new RedirectView("/timeline");
        } else {
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

    private List<WebPost> findUserPosts(String username) {
        List<WebPost> postlist = new ArrayList<>();
        return postlist;
    }

    @GetMapping("/timeline")
    public String timeline(Model model, @ModelAttribute("logincredentials") Credentials logincredentials) {
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
    public RedirectView timelinePostSubmit(@ModelAttribute WebPost webPost, @ModelAttribute("logincredentials") Credentials logincredentials, RedirectAttributes attributes) throws InterruptedException, IOException, ProcCallException {
        if (logincredentials.getUsername() == null) {
            return new RedirectView("/home");
        }
        // TODO: better error handling?
        return new RedirectView("/timeline");
    }

    @ModelAttribute("logincredentials")
    public Credentials logincredentials() {
        return new Credentials();
    }
}
