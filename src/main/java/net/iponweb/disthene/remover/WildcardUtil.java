package net.iponweb.disthene.remover;

public class WildcardUtil {

    public static String getPathsRegExFromWildcard(String wildcard) {
        return wildcard.replace(".", "\\.").replace("*", ".*").replace("{", "(")
                .replace("}", ")").replace(",", "|").replace("?", "[^\\.]");
    }

}
