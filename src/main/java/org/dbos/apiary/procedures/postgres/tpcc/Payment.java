/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package org.dbos.apiary.procedures.postgres.tpcc;

import org.dbos.apiary.benchmarks.tpcc.TPCCConstants;
import org.dbos.apiary.benchmarks.tpcc.TPCCUtil;
import org.dbos.apiary.benchmarks.tpcc.UserAbortException;
import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

public class Payment extends PostgresFunction {

    private static final Logger logger = LoggerFactory.getLogger(Payment.class);

    public static final String payUpdateWhseSQL =  "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE +
            "   SET W_YTD = W_YTD + ? " +
            " WHERE W_ID = ? ";

    public static final String payGetWhseSQL =  "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" +
            "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
            " WHERE W_ID = ?";

    public static final String payUpdateDistSQL =  "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
            "   SET D_YTD = D_YTD + ? " +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?";

    public static final String payGetDistSQL =  "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" +
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?";

    public static final String payGetCustSQL =  "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " +
            "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " +
            "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?";

    public static final String payGetCustCdataSQL =  "SELECT C_DATA " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?";

    public static final String payUpdateCustBalCdataSQL =  "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " +
            "       C_PAYMENT_CNT = ?, " +
            "       C_DATA = ? " +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?";

    public static final String payUpdateCustBalSQL =  "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " +
            "       C_PAYMENT_CNT = ? " +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?";

    public static final String payInsertHistSQL =  "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY +
            " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
            " VALUES (?,?,?,?,?,?,?,?)";

    public static final String customerByNameSQL =  "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " +
            "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
            "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_LAST = ? " +
            " ORDER BY C_FIRST";

    // Payment Txn
    public static int runFunction(PostgresContext ctxt, int w_id, int districtID, int customerID, int customerDistrictID, int customerWarehouseID, int customerByName, String customerLastName, String strPaymentAmount, String timestampMsStr) throws SQLException {
        double paymentAmount = Double.valueOf(strPaymentAmount);

        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;

        int result = ctxt.executeUpdate(payUpdateWhseSQL, paymentAmount, w_id);
        if (result == 0) {
            throw new UserAbortException("W_ID=" + w_id + " not found!");
        }

        ResultSet rs = ctxt.executeQuery(payGetWhseSQL, w_id);
        if (!rs.next()) {
            throw new UserAbortException("W_ID=" + w_id + " not found!");
        }
        w_street_1 = rs.getString("W_STREET_1");
        w_street_2 = rs.getString("W_STREET_2");
        w_city = rs.getString("W_CITY");
        w_state = rs.getString("W_STATE");
        w_zip = rs.getString("W_ZIP");
        w_name = rs.getString("W_NAME");
        rs.close();
        rs = null;

        result = ctxt.executeUpdate(payUpdateDistSQL, paymentAmount, w_id
        , districtID);
        if (result == 0) {
            throw new UserAbortException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");
        }

        rs = ctxt.executeQuery(payGetDistSQL, w_id, districtID);
        if (!rs.next()) {
            throw new UserAbortException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");
        }
        d_street_1 = rs.getString("D_STREET_1");
        d_street_2 = rs.getString("D_STREET_2");
        d_city = rs.getString("D_CITY");
        d_state = rs.getString("D_STATE");
        d_zip = rs.getString("D_ZIP");
        d_name = rs.getString("D_NAME");
        rs.close();
        rs = null;

        Customer c;
        if (customerByName > 0) {
            assert customerID <= 0;
            c = getCustomerByName(ctxt, customerWarehouseID, customerDistrictID, customerLastName);
        } else {
            assert customerLastName == null;
            c = getCustomerById(ctxt, customerWarehouseID, customerDistrictID, customerID);
        }

        c.c_balance -= paymentAmount;
        c.c_ytd_payment += paymentAmount;
        c.c_payment_cnt += 1;
        String c_data = null;
        if (c.c_credit.equals("BC")) { // bad credit
            rs = ctxt.executeQuery(payGetCustCdataSQL, customerWarehouseID, customerDistrictID, c.c_id);
            if (!rs.next()) {
                throw new UserAbortException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");
            }
            c_data = rs.getString("C_DATA");
            rs.close();
            rs = null;

            c_data = c.c_id + " " + customerDistrictID + " " + customerWarehouseID + " " + districtID + " " + w_id + " " + paymentAmount + " | " + c_data;
            if (c_data.length() > 500) {
                c_data = c_data.substring(0, 500);
            }

            result = ctxt.executeUpdate(payUpdateCustBalCdataSQL, c.c_balance, c.c_ytd_payment, c.c_payment_cnt, c_data, customerWarehouseID, customerDistrictID, c.c_id);

            if (result == 0) {
                throw new UserAbortException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID);
            }

        } else { // GoodCredit
            result = ctxt.executeUpdate(payUpdateCustBalSQL, c.c_balance, c.c_ytd_payment, c.c_payment_cnt, customerWarehouseID, customerDistrictID, c.c_id);

            if (result == 0)
                throw new UserAbortException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");

        }

        if (w_name.length() > 10)
            w_name = w_name.substring(0, 10);
        if (d_name.length() > 10)
            d_name = d_name.substring(0, 10);
        String h_data = w_name + "    " + d_name;

        Timestamp currTimestamp = TPCCUtil.convertTimestamp(Long.parseLong(timestampMsStr));
        ctxt.executeUpdate(payInsertHistSQL, customerDistrictID, customerWarehouseID, c.c_id, districtID, w_id, currTimestamp, paymentAmount, h_data);

        return 0;
    }

    public static Customer getCustomerById(PostgresContext ctxt, int c_w_id, int c_d_id, int c_id) throws SQLException {
        ResultSet rs = ctxt.executeQuery(payGetCustSQL, c_w_id, c_d_id, c_id);
        if (!rs.next()) {
            throw new UserAbortException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        Customer c = TPCCUtil.newCustomerFromResults(rs);
        c.c_id = c_id;
        c.c_last = rs.getString("C_LAST");
        rs.close();
        return c;
    }

    public static Customer getCustomerByName(PostgresContext ctxt, int c_w_id, int c_d_id, String customerLastName) throws SQLException {
        ArrayList<Customer> customers = new ArrayList<Customer>();

        ResultSet rs = ctxt.executeQuery(customerByNameSQL, c_w_id, c_d_id, customerLastName);

        while (rs.next()) {
            Customer c = TPCCUtil.newCustomerFromResults(rs);
            c.c_id = rs.getInt("C_ID");
            c.c_last = customerLastName;
            customers.add(c);
        }
        rs.close();

        if (customers.size() == 0) {
            throw new UserAbortException("C_LAST=" + customerLastName + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that
        // counts starting from 1.
        int index = customers.size() / 2;
        if (customers.size() % 2 == 0) {
            index -= 1;
        }
        return customers.get(index);
    }

    @Override
    public boolean isReadOnly() { return false; }
}
