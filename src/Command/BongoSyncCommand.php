<?php
/**
 * Created by PhpStorm.
 * User: Ahmad
 * Date: 7/25/2016
 * Time: 2:56 PM
 */
namespace DbSync\Command;

use Database\Connectors\ConnectionFactory;
use DbSync\ColumnConfiguration;
use DbSync\DbSync;
use DbSync\Hash\Md5Hash;
use DbSync\Table;
use DbSync\Transfer\Transfer;
use DbSync\WhereClause;
use Psr\Log\LogLevel;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\QuestionHelper;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Logger\ConsoleLogger;
use Symfony\Component\Console\Output\OutputInterface;

class BongoSyncCommand extends Command
{
    /**
     * @var OutputInterface
     */
    private $output;

    /**
     * @var InputInterface
     */
    private $input;

    /**
     * @var Questioner
     */
    private $questioner;

    protected function configure()
    {
        if(function_exists('posix_getpwuid'))
        {
            $currentUserInfo = posix_getpwuid(posix_geteuid());
            $currentUser = $currentUserInfo['name'];
        }else{
            $currentUser = null;
        }

        $this
            ->setName('bongodb-sync')
            ->setDescription('Sync a mysql database table from one host to another using an efficient checksum algorithm to find differences.')
            ->addArgument('source', InputArgument::REQUIRED, 'The source host ip to use.')
            ->addArgument('target', InputArgument::REQUIRED, 'The target host ip to use.')
            ->addArgument('table', InputArgument::OPTIONAL, 'The fully qualified database table to sync.')
            ->addOption('block-size','b', InputOption::VALUE_REQUIRED, 'The maximum block to use for when comparing.', 1024)
            ->addOption('charset',null, InputOption::VALUE_REQUIRED, 'The charset to use for database connections.', 'utf8')
            ->addOption('columns','c', InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, 'Columns to sync - all columns not "ignored" will be included if not specified. Primary key columns will be included automatically.')
            ->addOption('comparison','C', InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, 'Columns from the list of synced columns to use to create the hash - all columns not "ignored" will be included if not specified. Primary key columns will be included automatically.')
            ->addOption('config-file','f', InputOption::VALUE_REQUIRED, 'A path to a config.ini file from which to read values.', 'dbsync.ini')
            ->addOption('delete', null, InputOption::VALUE_NONE, 'Remove rows from the target table that do not exist in the source.')
            ->addOption('execute','e', InputOption::VALUE_NONE, 'Perform the data write on non-matching blocks.')
            ->addOption('help','h', InputOption::VALUE_NONE, 'Show this usage information.')
            ->addOption('ignore-columns','i', InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, 'Columns to ignore. Will not be copied or used to create the hash.')
            ->addOption('ignore-comparison','I', InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, 'Columns to ignore from the hash. Columns will still be copied.')
            ->addOption('password','p', InputOption::VALUE_OPTIONAL, 'The password for the specified user. Will be solicited on the tty if not given.')
            ->addOption('user','u', InputOption::VALUE_REQUIRED, 'The name of the user to connect with.', $currentUser)
            ->addOption('transfer-size','s', InputOption::VALUE_REQUIRED, 'The maximum copy size to use for when comparing.', 8)
            ->addOption('target.user',null , InputOption::VALUE_REQUIRED, 'The name of the user to connect to the target host with if different to the source.')
            ->addOption('target.table',null , InputOption::VALUE_REQUIRED, 'The name of the table on the target host if different to the source.')
            ->addOption('target.password',null , InputOption::VALUE_REQUIRED, 'The password for the target host if the target user is specified. Will be solicited on the tty if not given.')
            ->addOption('where', null , InputOption::VALUE_REQUIRED, 'A where clause to apply to the tables.')
            ->addOption('verbose', 'v' , InputOption::VALUE_NONE, 'Enable verbose output.')
            ->addOption('sourceDatabase', 'sd' , InputOption::VALUE_NONE, 'Source Database Name')
            ->addOption('targetDatabase', 'td' , InputOption::VALUE_NONE, 'Source Database Name')
        ;
    }

    /**
     * @param $option
     * @param $shortOption
     * @param null $message
     * @return mixed|string
     */
    protected function getPassword($option, $shortOption, $message = null)
    {
        $password = $this->input->getOption($option);

        $rawOptions = ["--$option"];
        $shortOption and $rawOptions[] = "-$shortOption";

        if(!$password && $this->input->hasParameterOption($rawOptions))
        {
            $password = $this->questioner->secret($message ?: "Enter your password: ");
        }

        return $password;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->output = $output;

        if($input->getOption('verbose'))
        {
            $this->output->setVerbosity(OutputInterface::VERBOSITY_DEBUG);
        }else {
            $this->output->setVerbosity(OutputInterface::VERBOSITY_VERY_VERBOSE);
        }

        $this->input = $input;

        if(($config = $this->input->getOption('config-file')) && is_file($config)){
            //print_r(parse_ini_file($config));
            $this->output->writeln("Reading ini file '$config'");
            foreach(parse_ini_file($config) as $name => $value){
                $this->input->setOption($name, $value);
            }
        }

        $this->questioner = new Questioner($input, $output, new QuestionHelper());

        $this->fire();
    }

    /**
     *
     */
    private function fire()
    {
        $charset = $this->input->getOption('charset');

        $user = $this->input->getOption('user');

        $password = $this->getPassword('password', 'p', "Enter password for local user '$user': ");

        if($remoteUser = $this->input->getOption('target.user'))
        {
            $remotePassword = $this->getPassword('target.password', null, "<info>Enter password for user '$remoteUser' on target host: </info>");
        }else{
            $remoteUser = $user;
            $remotePassword = $password;
        }

        $source = $this->createConnection($this->input->getArgument('source'), $user, $password, $charset);

        $target = $this->createConnection($this->input->getArgument('target'), $remoteUser, $remotePassword, $charset);

        $logger = new ConsoleLogger($this->output);

        $dryRun = $this->input->getOption('execute') ? false : true;

        if($dryRun)
        {
            $logger->notice("Dry run only. No data will be written to target.");
        }

        $sync = new DbSync(new Transfer(new Md5Hash(), $this->input->getOption('block-size'), $this->input->getOption('transfer-size')));

        $sync->dryRun($dryRun);

        //$sync->delete($this->input->getOption('delete'));

        $sync->setLogger($logger);

        $this->syncTables($source, $target, $sync, $logger);
    }

    private function parseTableName($name)
    {
        return explode('.', $name, 2);
    }

    private function createConnection($host, $user, $password, $charset)
    {
        return (new ConnectionFactory())->make([
            'host'      => $host,
            'username'  => $user,
            'password'  => $password,
            'charset'   => $charset,
            'collation' => 'utf8_general_ci',
            'driver'    => 'mysql',

            'options' => [
                \PDO::ATTR_ERRMODE               => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_DEFAULT_FETCH_MODE    => \PDO::FETCH_ASSOC,
                \PDO::ATTR_EMULATE_PREPARES      => false,
            ]
        ]);
    }

    /**
     * @param $source
     * @param $target
     * @param $sync
     * @param $logger
     */
    private function syncTables($source, $target, $sync, $logger)
    {
        $sourceDatabase = $this->input->getOption('sourceDatabase');
        $targetDatabase = $this->input->getOption('targetDatabase');
        foreach($this->tablesToSync as $tableToSync) {
            $sourceTable = $targetTable = $tableToSync['tableName'];
            $igonreColumns = isset($tableToSync['ignoreColumns']) ? $tableToSync['ignoreColumns'] : [];
            $sourceTableObj = new Table($source, $sourceDatabase, $sourceTable);
            $destTableObj = new Table($target, $targetDatabase, $targetTable);
            if($sourceTable == "synopsis") {
                $sync->setTransferSize(8);
            } else {
                $sync->setTransferSize($this->input->getOption('transfer-size'));
            }
            $result = $sync->sync(
                $sourceTableObj,
                $destTableObj,
                new ColumnConfiguration($this->input->getOption('columns'), $igonreColumns),
                new ColumnConfiguration($this->input->getOption('comparison'), $this->input->getOption('ignore-comparison'))
            );

            $logger->notice(json_encode($result->toArray()));
        }

    }

    private $tablesToSync = [
        [
            'tableName' => 'content',
            'ignoreColumns' => ['views', 'plays']
        ],
        [
            'tableName' => 'episodes',
            'ignoreColumns' => ['views', 'plays']
        ],
        [
            'tableName' => 'category',
        ],
        [
            'tableName' => 'albums',
        ],
        [
            'tableName' => 'albums_to_contents',
        ],
        [
            'tableName' => 'artist',
        ],

        [
            'tableName' => 'banners',
        ],
        [
            'tableName' => 'bucket',
        ],[
            'tableName' => 'cast_and_crew',
        ],[
            'tableName' => 'cast_and_crews_to_artists',
        ],
        [
            'tableName' => 'channel_selector',
        ],
        [
            'tableName' => 'channel_selector_detail',
        ],[
            'tableName' => 'content_genre',
        ],[
            'tableName' => 'content_owner',
        ],
        [
            'tableName' => 'content_parent_child',
        ],
        [
            'tableName' => 'genre',
        ],
        [
            'tableName' => 'live_tv',
        ],
        [
            'tableName' => 'media',
        ],
        [
            'tableName' => 'page_manager',
        ],
        [
            'tableName' => 'pagemanager_banner',
        ],
        [
            'tableName' => 'pagemanager_slider',
        ],
        [
            'tableName' => 'pages',
        ],
        [
            'tableName' => 'platform_pages',
        ],
        [
            'tableName' => 'platforms',
        ],
        [
            'tableName' => 'publish_media',
        ],
        [
            'tableName' => 'publish_tags',
        ],
        [
            'tableName' => 'role',
        ],
        [
            'tableName' => 'slider_details',
        ],
        [
            'tableName' => 'sliders',
        ],
        [
            'tableName' => 'slides',
        ],

        [
            'tableName' => 'tags',
        ],
        [
            'tableName' => 'synopsis',
        ],
    ];
}
