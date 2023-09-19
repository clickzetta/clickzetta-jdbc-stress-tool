package com.clickzetta.jdbc_stress_tool;

import com.clickzetta.client.jdbc.core.CZUtil;

import java.util.List;

public class SqlUtils {
    public static boolean isLocal(String input) {
        input = CZUtil.stripLeadingComment(input).toLowerCase().trim();
        return input.startsWith("use") || input.startsWith("set") ||
                input.startsWith("clear") || input.startsWith("print");
    }

    public static List<String> splitSql(String query) {
        return CZUtil.splitSql(query);
    }
}
