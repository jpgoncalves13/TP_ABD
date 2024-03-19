import com.beust.jcommander.Parameter;

public class Options {
    @Parameter(names = {"-h","-?","--help"}, help = true, description = "display usage information")
    public boolean help;

    @Parameter(names = {"-d","--database"}, description = "JDBC database url")
    public String database = "jdbc:postgresql://localhost/stack";

    @Parameter(names = {"-U","--user"}, description = "database user name")
    public String user = "postgres";

    @Parameter(names = {"-P","--password"}, description = "database password")
    public String passwd = "postgres";

    @Parameter(names = {"-W","--warmup"}, description = "warmup time (seconds)")
    public int warmup = 15;

    @Parameter(names = {"-R","--runtime"}, description = "run time (seconds)")
    public int runtime = 180;

    @Parameter(names = {"-c","--clients"}, description = "number of client threads")
    public int clients = 1;
}
