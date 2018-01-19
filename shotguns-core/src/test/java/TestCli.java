import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class TestCli {

    public static void main(String[] args) throws Throwable{


        CommandLineParser parser = new DefaultParser( );
        Options options = new Options();
        options.addOption("h", "help", false, "Print this usage information");
        options.addOption("v", "verbose", false, "Print out VERBOSE information" );
        options.addOption("f", "file", true, "File to save program output to");

//        CommandLine commandLine = parser.parse( options, args );
//        System.out.println(commandLine.hasOption("h"));

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "AntOptsCommonsCLI", options );
    }
}
