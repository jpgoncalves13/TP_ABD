import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class Utils {

    private static Random random = new Random();
    private static char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ".toCharArray();


    public static String randomString(int n) {
        return IntStream.range(0, n)
                .mapToObj(x -> chars[random.nextInt(chars.length)])
                .map(Object::toString)
                .reduce((acc, x) -> acc + x)
                .get();
    }


    @SafeVarargs
    public static <T> T randomElement(List<T> ...lists) {
        var list = lists[random.nextInt(lists.length)];
        return list.get(random.nextInt(list.size()));
    }
}
