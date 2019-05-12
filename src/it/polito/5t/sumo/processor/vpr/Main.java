package it.polito.5t.sumo.processor.vpr;

import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    static final float THRESHOLD_LOW_PR = 300;
    static final String LOW_PR_0 = "1";
//    static final String LOW_PR_1 = "8";
//    static final String LOW_PR_2 = "5";
    static final float THRESHOLD_MEDIUM_PR = 600;
    static final String MEDIUM_PR_0 = "5";
//    static final String MEDIUM_PR_1 = "10";
//    static final String MEDIUM_PR_2 = "7.5";
    static final float THRESHOLD_HIGH_PR = Float.MAX_VALUE;
    static final String HIGH_PR_0 = "10";
//    static final String HIGH_PR_1 = "12";
//    static final String HIGH_PR_2 = "15";
    static String names[] = {"M1", "M2",
        "M3", "M4", "M5", "M6",
        "M7", "Tu1", "Tu2", "Tu3",
        "Tu4", "Tu5", "Tu6", "Tu7",
        "W1", "W2", "W3", "W4",
        "W5", "W6", "W7", "Th1",
        "Th2", "Th3", "Th4",
        "Th5", "Th6", "Th7", "F1",
        "F2", "F3", "F4", "F5",
        "F6", "F7", "Sa1",
        "Sa2", "Sa3", "Sa4",
        "Sa5", "Sa6", "Sa7", "Su1", "Su2",
        "Su3", "Su4", "Su5", "Su6",
        "Su7", "H1", "H2", "H3",
        "H4", "H5", "H6", "H7"};
//    static String dates[] = {"2017-10-02",
//        "2017-11-06", "2017-12-04", "2018-01-15",
//        "2018-02-12", "2018-03-05", "2018-04-09", "2017-10-10",
//        "2017-11-07", "2017-12-05", "2018-01-16", "2018-02-13",
//        "2018-03-06", "2018-04-03", "2017-10-11", "2017-11-08",
//        "2017-12-06", "2018-01-17", "2018-02-14", "2018-03-07",
//        "2018-04-04", "2017-10-12", "2017-11-02", "2017-12-07",
//        "2018-01-04", "2018-02-15", "2018-03-01", "2018-04-05", "2017-10-13",
//        "2017-11-03", "2017-12-01", "2018-01-05", "2018-02-02", "2018-03-02",
//        "2018-04-06", "2017-10-07", "2017-11-04", "2017-12-02", "2018-01-13",
//        "2018-02-03", "2018-03-03", "2018-04-07", "2017-10-01",
//        "2017-11-05", "2017-12-03", "2018-01-14", "2018-02-11",
//        "2018-03-04", "2018-04-08", "2017-11-01", "2017-12-08",
//        "2017-12-25", "2017-12-26", "2018-03-30", "2018-04-01", "2018-04-02"};
    static String seeds[] = {"100",
        "200", "300", "400", "500", "600", "700", "27000",
        "28000", "29000", "30000", "31000",
        "32000", "33000", "34000", "35000",
        "36000", "37000", "38000", "39000",
        "40000", "41000", "42000", "43000",
        "44000", "45000", "46000", "47000",
        "800", "900", "1000", "2000", "3000", "4000", "5000", "13000",
        "14000", "15000", "16000", "17000",
        "18000", "19000", "20000", "21000",
        "22000", "23000", "24000", "25000",
        "26000", "6000", "7000", "8000",
        "9000", "10000", "11000", "12000"};

    static int STEP_SIZE = 60 * 60;
    static int PENETRATION_RATE = -1;
    static int PENETRATION_RATE_OLD = -1;
    static int SEED = -1;
    static String SIM_NAME = "";

    static String INPUT_TABLE_NAME = "";
    static String OUTPUT_TABLE_NAME = "";

    public static void main(String[] args) {
        String NEW_PR = LOW_PR_0 + "-" + MEDIUM_PR_0 + "-" + HIGH_PR_0;
//        String NEW_PR = LOW_PR_0 + "-" + LOW_PR_1 + "-" + MEDIUM_PR_1 + "-" + HIGH_PR_1 + "-" + HIGH_PR_2 + "R";
        for (int i = 0; i < names.length; i++) {
            try {
                System.out.println((i + 1) + "/" + names.length + " " + names[i]);
                DBHelper dbHelper = new DBHelper();
                List<Float> hourlyAvgFlow = dbHelper.queryHourlyAvgFlow(names[i], "100", "60");
                int stepFrom = 0;
                int stepTo = STEP_SIZE;
                Random rand = new Random(Long.valueOf(seeds[i]));

                for (float flow : hourlyAvgFlow) {
                // UNCOMMENT ONE OF THE FOLLOWING BLOCKS

                //     TRUE RANDOM BLOCK
                //    String local_pr = "";
                //    switch (rand.nextInt(5)) {
                //        case 0:
                //            local_pr = LOW_PR_0;
                //            break;
                //        case 1:
                //            local_pr = LOW_PR_1;
                //            break;
                //        case 2:
                //            local_pr = MEDIUM_PR_1;
                //            break;
                //        case 3:
                //            local_pr = HIGH_PR_1;
                //            break;
                //        case 4:
                //            local_pr = HIGH_PR_2;
                //            break;
                //    }
                //    dbHelper.selectAndInsertFromProcessedAveragespeed(names[i],  local_pr, "60", stepFrom, stepTo, NEW_PR);
                //    dbHelper.selectAndInsertFromProcessedDeltaTime(names[i], local_pr, "60", stepFrom, stepTo, NEW_PR);
                //    System.out.println("\t" + stepFrom + " to " + stepTo + "\t" + flow + "v/h\t" + local_pr + " -> " + NEW_PR);
                //     TRUE RANDOM BLOCK

                //     RANDOM wrt TRAFFIC BLOCK
                //    if (flow < THRESHOLD_LOW_PR) {
                //        String local_low_pr = "";
                //        switch (rand.nextInt(3)) {
                //            case 0:
                //                local_low_pr = LOW_PR_0;
                //                break;
                //            case 1:
                //                local_low_pr = LOW_PR_1;
                //                break;
                //            case 2:
                //                local_low_pr = LOW_PR_2;
                //                break;
                //        }
                //        dbHelper.selectAndInsertFromProcessedAveragespeed(names[i], local_low_pr, "60", stepFrom, stepTo, NEW_PR);
                //        dbHelper.selectAndInsertFromProcessedDeltaTime(names[i], local_low_pr, "60", stepFrom, stepTo, NEW_PR);
                //        System.out.println("\t" + stepFrom + " to " + stepTo + "\t" + flow + "v/h\tLOW\t" + local_low_pr + " -> " + NEW_PR);
                //    } else if (flow < THRESHOLD_MEDIUM_PR & flow >= THRESHOLD_LOW_PR) {
                //        String local_medium_pr = "";
                //        switch (rand.nextInt(3)) {
                //            case 0:
                //                local_medium_pr = MEDIUM_PR_0;
                //                break;
                //            case 1:
                //                local_medium_pr = MEDIUM_PR_1;
                //                break;
                //            case 2:
                //                local_medium_pr = MEDIUM_PR_2;
                //                break;
                //        }
                //        dbHelper.selectAndInsertFromProcessedAveragespeed(names[i], local_medium_pr, "60", stepFrom, stepTo, NEW_PR);
                //        dbHelper.selectAndInsertFromProcessedDeltaTime(names[i], local_medium_pr, "60", stepFrom, stepTo, NEW_PR);
                //        System.out.println("\t" + stepFrom + " to " + stepTo + "\t" + flow + "v/h\tMEDIUM\t" + local_medium_pr + " -> " + NEW_PR);
                //    } else if (flow < THRESHOLD_HIGH_PR & flow >= THRESHOLD_MEDIUM_PR) {
                //        String local_high_pr = "";
                //        switch (rand.nextInt(3)) {
                //            case 0:
                //                local_high_pr = HIGH_PR_0;
                //                break;
                //            case 1:
                //                local_high_pr = HIGH_PR_1;
                //                break;
                //            case 2:
                //                local_high_pr = HIGH_PR_2;
                //                break;
                //        }
                //        dbHelper.selectAndInsertFromProcessedAveragespeed(names[i], local_high_pr, "60", stepFrom, stepTo, NEW_PR);
                //        dbHelper.selectAndInsertFromProcessedDeltaTime(names[i], local_high_pr, "60", stepFrom, stepTo, NEW_PR);
                //        System.out.println("\t" + stepFrom + " to " + stepTo + "\t" + flow + "v/h\tHIGH\t" + local_high_pr + " -> " + NEW_PR);
                //    }
                //     RANDOM wrt TRAFFIC

                // BASED ON TRAFFIC _ NO RANDOM
                // if (flow < THRESHOLD_LOW_PR) {
                //     String local_low_pr = "";
                //     local_low_pr = LOW_PR_0;
                //     dbHelper.selectAndInsertFromProcessedAveragespeed(names[i], local_low_pr, "60", stepFrom, stepTo, NEW_PR);
                //     dbHelper.selectAndInsertFromProcessedDeltaTime(names[i], local_low_pr, "60", stepFrom, stepTo, NEW_PR);
                //     System.out.println("\t" + stepFrom + " to " + stepTo + "\t" + flow + "v/h\tLOW\t" + local_low_pr + " -> " + NEW_PR);
                // } else if (flow < THRESHOLD_MEDIUM_PR & flow >= THRESHOLD_LOW_PR) {
                //     String local_medium_pr = "";
                //     local_medium_pr = MEDIUM_PR_0;
                //     dbHelper.selectAndInsertFromProcessedAveragespeed(names[i], local_medium_pr, "60", stepFrom, stepTo, NEW_PR);
                //     dbHelper.selectAndInsertFromProcessedDeltaTime(names[i], local_medium_pr, "60", stepFrom, stepTo, NEW_PR);
                //     System.out.println("\t" + stepFrom + " to " + stepTo + "\t" + flow + "v/h\tMEDIUM\t" + local_medium_pr + " -> " + NEW_PR);
                // } else if (flow < THRESHOLD_HIGH_PR & flow >= THRESHOLD_MEDIUM_PR) {
                //     String local_high_pr = "";
                //     local_high_pr = HIGH_PR_0;
                //     dbHelper.selectAndInsertFromProcessedAveragespeed(names[i], local_high_pr, "60", stepFrom, stepTo, NEW_PR);
                //     dbHelper.selectAndInsertFromProcessedDeltaTime(names[i], local_high_pr, "60", stepFrom, stepTo, NEW_PR);
                //     System.out.println("\t" + stepFrom + " to " + stepTo + "\t" + flow + "v/h\tHIGH\t" + local_high_pr + " -> " + NEW_PR);
                // }
                // BASED ON TRAFFIC _ NO RANDOM

                    stepFrom += STEP_SIZE;
                    stepTo += STEP_SIZE;
                }
                dbHelper.jdbcClose();

            } catch (SQLException ex) {
                Logger.getLogger(Main.class
                        .getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
