package com.booking.validation;

import static java.lang.StrictMath.abs;

/**
 * Created by lezhong on 7/14/16.
 */


public class Validating {
    /*
        MySQL type list:
            char,
            varchar,
            tinyext,
            text,
            blob,
            mediumtext,
            mediumblob,
            longtext,
            longblob,
            enum(x, y, z, etc.)
            set

        HBase type list:
            row key, column family, column qualifier, timestamp, value
    */

    Boolean compareGeneral(String valueMySQL, String valueHBase) {
        // Double, TinyInt, SmallInt, MediumInt, Int, BigInt, Decimal, Enum, Set,
        return valueMySQL.equals(valueHBase);
    }

    Boolean compareFloat(String valueMySQL, String valueHBase) {
        int nrDigits = valueMySQL.split(".")[1].length();
        float floatMySQL = Float.parseFloat(valueMySQL);
        float floatHBase;
        if (nrDigits > 0) {
            floatHBase = Float.parseFloat(valueMySQL.split(".")[0] + valueMySQL.split(".")[1].substring(0, nrDigits));
        } else {
            floatHBase = Float.parseFloat(valueHBase);
        }

        return abs(floatHBase - floatMySQL) < 0.0001;
    }

    Boolean compareChar(String valueMySQL, String valueHBase) {
        // TODO: encoding check
        return valueMySQL.equals(valueHBase);
    }

    Boolean comapreVarchar(String valueMySQL, String valueHBase) {

        return true;
    }

    Boolean compareText(String valueMySQL, String valueHBase) {

        return true;
    }

    Boolean compareTimestamp(String valueMySQL, String valueHBase) {

        return true;
    }

    Boolean compareDate(String valueMySQL, String valueHBase) {

        return true;
    }
}
