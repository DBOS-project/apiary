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

import org.dbos.apiary.benchmarks.tpcc.TPCCConfig;
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
import java.util.List;

public class NewOrder extends PostgresFunction {

    private static final Logger logger = LoggerFactory.getLogger(NewOrder.class);
    public static final String stmtGetCustSQL =
            "SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = ? " +
                    "   AND C_D_ID = ? " +
                    "   AND C_ID = ?";

    public static final String stmtGetWhseSQL =
            "SELECT W_TAX " +
                    "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
                    " WHERE W_ID = ?";

    public static final String stmtGetDistSQL =
            "SELECT D_NEXT_O_ID, D_TAX " +
                    "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
                    " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE";

    public static final String  stmtInsertNewOrderSQL =
            "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
                    " (NO_O_ID, NO_D_ID, NO_W_ID) " +
                    " VALUES ( ?, ?, ?)";

    public static final String  stmtUpdateDistSQL =
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
                    "   SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
                    " WHERE D_W_ID = ? " +
                    "   AND D_ID = ?";

    public static final String  stmtInsertOOrderSQL =
            "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
                    " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?)";

    public static final String  stmtGetItemSQL =
            "SELECT I_PRICE, I_NAME , I_DATA " +
                    "  FROM " + TPCCConstants.TABLENAME_ITEM +
                    " WHERE I_ID = ?";

    public static final String  stmtGetStockSQL =
            "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
                    "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
                    "  FROM " + TPCCConstants.TABLENAME_STOCK +
                    " WHERE S_I_ID = ? " +
                    "   AND S_W_ID = ? FOR UPDATE";

    public static final String  stmtUpdateStockSQL =
            "UPDATE " + TPCCConstants.TABLENAME_STOCK +
                    "   SET S_QUANTITY = ? , " +
                    "       S_YTD = S_YTD + ?, " +
                    "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
                    "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
                    " WHERE S_I_ID = ? " +
                    "   AND S_W_ID = ?";

    public static final String  stmtInsertOrderLineSQL =
            "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
                    " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
                    " VALUES (?,?,?,?,?,?,?,?,?)";


    public static int runFunction(PostgresContext ctxt, int w_id, int d_id, int c_id, int o_ol_cnt, int o_all_local, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, String timestampStr) throws SQLException {
        float c_discount, w_tax, d_tax = 0, i_price;
        int d_next_o_id, o_id = -1, s_quantity;
        String c_last = null, c_credit = null, i_name, i_data, s_data;
        String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
        String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
        float[] itemPrices = new float[o_ol_cnt];
        float[] orderLineAmounts = new float[o_ol_cnt];
        String[] itemNames = new String[o_ol_cnt];
        int[] stockQuantities = new int[o_ol_cnt];
        char[] brandGeneric = new char[o_ol_cnt];
        int ol_supply_w_id, ol_i_id, ol_quantity;
        int s_remote_cnt_increment;
        float ol_amount, total_amount = 0;

        try {
            ResultSet rs = ctxt.executeQuery(stmtGetCustSQL, w_id, d_id, c_id);
            if (!rs.next())
                throw new UserAbortException("C_D_ID=" + d_id
                        + " C_ID=" + c_id + " not found!");
            c_discount = rs.getFloat("C_DISCOUNT");
            c_last = rs.getString("C_LAST");
            c_credit = rs.getString("C_CREDIT");
            rs.close();
            rs = null;

            rs = ctxt.executeQuery(stmtGetWhseSQL, w_id);
            if (!rs.next())
                throw new UserAbortException("W_ID=" + w_id + " not found!");
            w_tax = rs.getFloat("W_TAX");
            rs.close();
            rs = null;

            rs = ctxt.executeQuery(stmtGetDistSQL, w_id, d_id);
            if (!rs.next()) {
                throw new UserAbortException("D_ID=" + d_id + " D_W_ID=" + w_id
                        + " not found!");
            }
            d_next_o_id = rs.getInt("D_NEXT_O_ID");
            d_tax = rs.getFloat("D_TAX");
            rs.close();
            rs = null;

            //woonhak, need to change order because of foreign key constraints
            //update next_order_id first, but it might doesn't matter
            int result = ctxt.executeUpdate(stmtUpdateDistSQL, w_id, d_id);
            if (result == 0)
                throw new UserAbortException(
                        "Error!! Cannot update next_order_id on district for D_ID="
                                + d_id + " D_W_ID=" + w_id);

            o_id = d_next_o_id;

            // woonhak, need to change order, because of foreign key constraints
            //[[insert ooder first
            Timestamp currTimestamp = TPCCUtil.convertTimestamp(Long.parseLong(timestampStr));
            ctxt.executeUpdate(stmtInsertOOrderSQL, o_id, d_id, w_id, c_id, currTimestamp, o_ol_cnt, o_all_local);
            //insert ooder first]]
            /*TODO: add error checking */

            ctxt.executeUpdate(stmtInsertNewOrderSQL, o_id, d_id, w_id);
            /*TODO: add error checking */

            List<Object[]> updateStockInputs = new ArrayList<>();
            List<Object[]> insertOrderLineInputs = new ArrayList<>();
            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
                ol_i_id = itemIDs[ol_number - 1];
                ol_quantity = orderQuantities[ol_number - 1];
                rs = ctxt.executeQuery(stmtGetItemSQL, ol_i_id);
                if (!rs.next()) {
                    // This is (hopefully) an expected error: this is an
                    // expected new order rollback
                    assert ol_number == o_ol_cnt;
                    assert ol_i_id == TPCCConfig.INVALID_ITEM_ID;
                    rs.close();
                    throw new UserAbortException(
                            "EXPECTED new order rollback: I_ID=" + ol_i_id
                                    + " not found!");
                }

                i_price = rs.getFloat("I_PRICE");
                i_name = rs.getString("I_NAME");
                i_data = rs.getString("I_DATA");
                rs.close();
                rs = null;

                itemPrices[ol_number - 1] = i_price;
                itemNames[ol_number - 1] = i_name;

                rs = ctxt.executeQuery(stmtGetStockSQL, ol_i_id, ol_supply_w_id);
                if (!rs.next())
                    throw new UserAbortException("I_ID=" + ol_i_id
                            + " not found!");
                s_quantity = rs.getInt("S_QUANTITY");
                s_data = rs.getString("S_DATA");
                s_dist_01 = rs.getString("S_DIST_01");
                s_dist_02 = rs.getString("S_DIST_02");
                s_dist_03 = rs.getString("S_DIST_03");
                s_dist_04 = rs.getString("S_DIST_04");
                s_dist_05 = rs.getString("S_DIST_05");
                s_dist_06 = rs.getString("S_DIST_06");
                s_dist_07 = rs.getString("S_DIST_07");
                s_dist_08 = rs.getString("S_DIST_08");
                s_dist_09 = rs.getString("S_DIST_09");
                s_dist_10 = rs.getString("S_DIST_10");
                rs.close();
                rs = null;

                stockQuantities[ol_number - 1] = s_quantity;

                if (s_quantity - ol_quantity >= 10) {
                    s_quantity -= ol_quantity;
                } else {
                    s_quantity += -ol_quantity + 91;
                }

                if (ol_supply_w_id == w_id) {
                    s_remote_cnt_increment = 0;
                } else {
                    s_remote_cnt_increment = 1;
                }

                Object[] stockInput = new Object[5];
                stockInput[0] = s_quantity;
                stockInput[1] = ol_quantity;
                stockInput[2] = s_remote_cnt_increment;
                stockInput[3] = ol_i_id;
                stockInput[4] = ol_supply_w_id;
                updateStockInputs.add(stockInput);

                ol_amount = ol_quantity * i_price;
                orderLineAmounts[ol_number - 1] = ol_amount;
                total_amount += ol_amount;

                if (i_data.indexOf("ORIGINAL") != -1
                        && s_data.indexOf("ORIGINAL") != -1) {
                    brandGeneric[ol_number - 1] = 'B';
                } else {
                    brandGeneric[ol_number - 1] = 'G';
                }

                switch ((int) d_id) {
                    case 1:
                        ol_dist_info = s_dist_01;
                        break;
                    case 2:
                        ol_dist_info = s_dist_02;
                        break;
                    case 3:
                        ol_dist_info = s_dist_03;
                        break;
                    case 4:
                        ol_dist_info = s_dist_04;
                        break;
                    case 5:
                        ol_dist_info = s_dist_05;
                        break;
                    case 6:
                        ol_dist_info = s_dist_06;
                        break;
                    case 7:
                        ol_dist_info = s_dist_07;
                        break;
                    case 8:
                        ol_dist_info = s_dist_08;
                        break;
                    case 9:
                        ol_dist_info = s_dist_09;
                        break;
                    case 10:
                        ol_dist_info = s_dist_10;
                        break;
                }

                Object[] insertOLInput = new Object[9];
                insertOLInput[0] = o_id;
                insertOLInput[1] = d_id;
                insertOLInput[2] = w_id;
                insertOLInput[3] = ol_number;
                insertOLInput[4] = ol_i_id;
                insertOLInput[5] = ol_supply_w_id;
                insertOLInput[6] = ol_quantity;
                insertOLInput[7] = ol_amount;
                insertOLInput[8] = ol_dist_info;
                insertOrderLineInputs.add(insertOLInput);

            } // end-for

            ctxt.insertMany(stmtInsertOrderLineSQL, insertOrderLineInputs);
            ctxt.insertMany(stmtUpdateStockSQL, updateStockInputs);

            total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
        } catch(UserAbortException userEx) {
            logger.debug("Caught an expected error in New Order");
            throw userEx;
        }

        return 0;
    }
    @Override
    public boolean isReadOnly() { return false; }
}
