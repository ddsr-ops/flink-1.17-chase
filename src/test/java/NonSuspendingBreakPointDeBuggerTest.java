import java.util.ArrayList;
import java.util.List;

/**
 * @author ddsr, created it at 2023/8/23 9:12
 */
public class NonSuspendingBreakPointDeBuggerTest {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        Integer a = 5;
        Integer b = 4;
        Integer c = a + b;
        list.forEach(System.out::println);
    }
}
