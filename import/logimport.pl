#!/usr/bin/perl -w

##############################################################################
# Handles log file raw import - Step 1 of importing the data.
#
# Reads out the logfile and writes the data into the 'data' table.
#
# Continues with 'searchdir.pl' when finished.
#
# rleuthold@access.ch - 8.1.2009
##############################################################################

use strict;
use DBI;
use POSIX;
use Fcntl;
use Data::Dumper;
use Date::Calc qw(:all);
use File::stat;
use IO::File;

use lib 'lib';
use lib::DBHandler;
use lib::XMLPaths;
use lib::DBTables;

# VARIABLES
my @ARGS;
my @STARTTIME;
my @ENDTIME;
my $RESULTFILE;

# counters
my $DATACOUNT	= 0;

##############################################################################

##
# Global variables
my $DBH; 
my $TABLES		= DBTables->new();
my $PATHS		= XMLPaths->new();
my $PERLCONFIG	= PerlConfig->new();

# Paths / directories
my $DATA_PATH 	= $PATHS->get_path('data');
my $IMPORTED_FOLDER 	= $PATHS->get_path('imported');
my $IMPORTED_LOGS_FOLDER = $PATHS->get_path('importedlogs');

my $SCRIPT_PATH	= $PERLCONFIG->get_scriptsfolder();
my $DB			= DBHandler->new->{DB};

# database tables
my $TABLE_DATA	= $TABLES->get_table_name('data');
my $TABLE_LOGS	= $TABLES->get_table_name('logfiles');
my $TABLE_RFIDS	= $TABLES->get_table_name('rfids');

my $DAYS_TO_COUNT_TABLE = $TABLES->get_days_to_count_table();

my $BACKUPUSER	= $PERLCONFIG->get_dbbackupuser();
my $DBBACKUPDIR	= $PERLCONFIG->get_dbbackupdirectory();

my $DATEFORMAT	= "20%02d-%02d-%02d %02d:%02d:%02d";
my $DAY_FOLDER;

####################################
# PREAMBLE
####################################

print"\n================================================\n";
print"STARTING LOGIMPORT.PL";
print"\n================================================\n";


my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime time;
$year += 1900;
$mon++;

###############################################
# Creating needed folders

umask(000); 									# UNIX file permission junk
(mkdir($IMPORTED_FOLDER, 0771) || die ("Could not create folder $IMPORTED_FOLDER: $!")) unless (-d $IMPORTED_FOLDER);

my $dayDate = $mday."_". $mon ."_". $year ."/";
$DAY_FOLDER = $IMPORTED_FOLDER . $dayDate;
(mkdir($DAY_FOLDER, 0771) || die ("Could not create folder $DAY_FOLDER: $!")) unless (-d $DAY_FOLDER);		

(mkdir($IMPORTED_LOGS_FOLDER, 0771) || die ("Could not create folder $IMPORTED_LOGS_FOLDER: $!")) unless (-d $IMPORTED_LOGS_FOLDER);

##
# setting up file for output
my $filename = $DAY_FOLDER . "logimport_log\.txt";

##
# opening file for writing
sysopen (RES, $filename, O_CREAT |O_WRONLY, 0755) or die("Can't open file $filename : $!");

#############################################################################
# open db connection
$DBH = DBHandler->new()->connect();
#############################################################################

###############################################
# Create 'temporary' table to store the days which have to be counted in the counter.pl script.
$DBH->do(qq{DROP TABLE IF EXISTS $DAYS_TO_COUNT_TABLE}) || die ("Could not drop table '$DAYS_TO_COUNT_TABLE': " . $DBH->errstr);
$DBH->do(qq{CREATE TABLE `$DAYS_TO_COUNT_TABLE` (`day` date NOT NULL, PRIMARY KEY (`day`) )}) || die ("Could not create table '$DAYS_TO_COUNT_TABLE': " . $DBH->errstr);

##
# get logfiles
my @logFiles = map {/(\d{8}_\d{6}\.txt)/} <$DATA_PATH* .txt>;

##
# getting the time info (yy-mmddhhmnss - readable by the Datemanip module) out of the filename
my %logFiles;
foreach (@logFiles) {							
	my $log = $_;
	my ($y, $m, $d, $h, $min, $sec) = /\d{2}(\d{2})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.txt/;
	
	$logFiles{$y."-".$m.$d.$h.$min.$sec} = $log;
}

print "======================================================\n";
printf RES "======================================================\n";

if( keys %logFiles == 0) {
	print"No Files to import for today ... bye\n";
	printf RES"No Files to import for today ... bye\n";	
	exit;
} else {
	print "Files to import:\n";
	printf RES "Files to import:\n";
	
	my $i = 1;
	for my $file( sort keys %logFiles ) {
		print "\t$i.)\t$file => $logFiles{$file}\n";
		printf RES "\t$i.)\t$file => $logFiles{$file}\n";		
		$i++;
	}
	
	# db backup backup
	( mkdir($DBBACKUPDIR, 0771) || die("Cannot create database backup directory: $!") ) unless (-d $DBBACKUPDIR); # create backup dir if necessary
	my $dbbackupfile = $DBBACKUPDIR . 'micedata_'. $year . '_' . $mon . '_' . $mday . '.sql.bz2';

	print "\n======================================================\n";	
	printf RES "\n======================================================\n";	
	
	if (-e $dbbackupfile) {
		print "\nBackup file '$dbbackupfile' for today already exists, skipping backup ...\n";
		printf RES "\nBackup file '$dbbackupfile' for today already exists, skipping backup ...\n";		
	} else {
		print "\nBacking up database '$DB' to file '$dbbackupfile' ...\n";
		printf RES "\nBacking up database '$DB' to file '$dbbackupfile' ...\n";		
	
		my @ARGS = ("/usr/bin/mysqldump --opt -u $BACKUPUSER --lock-tables $DB | /bin/bzip2 -c > $dbbackupfile");
		system(@ARGS) == 0
			or die "Backup process '@ARGS' failed: $?";
	
		print "... backup complete.\n";	
		printf RES "... backup complete.\n";			
		
	}
	
	print "------------------------------------------------------\n";
	printf RES "------------------------------------------------------\n";	
}

##############################################################	           
# MAIN
##############################################################	   
#
## Now that we have the log time in the key and the LOGFILE name in the value let's start
for my $logFile( sort keys %logFiles ) {

	
	#print $STARTTIME ."\n";
	my $fileName	= $logFiles{$logFile};
	
	next if (&CheckImport($fileName) == 1);		# check if the file has already been imported and skip if it is so
	

	
	###################################
	# send each file to the main loop
	###################################
	&File($logFile,$fileName);					# send each file in the main loop
	
	##
	# updating logs table
	my $size 		= ((stat($DATA_PATH.$fileName)->size)/1024);
	
	my $logSQL = qq {INSERT INTO $TABLE_LOGS(logfile, short, start, end, size, duration,import) 
						VALUES(
							'$fileName',
							'$logFile', 
							(SELECT MIN(time) FROM $TABLE_DATA WHERE import='$logFile' ),
							(SELECT MAX(time) FROM $TABLE_DATA WHERE import='$logFile' ),
							'$size', 
							(SELECT TIMEDIFF((select max(time) from data where import='$logFile'),(select min(time) from data where import='$logFile'))),
							NOW()) 
					};
		
		
	$DBH->do($logSQL)
		or die("Could not update or insert value: " . $DBH->errstr );
		
		
	##
	# Get the days for this log file has data for and insert them into the table with the days to count
	$DBH->do(qq{INSERT IGNORE INTO $DAYS_TO_COUNT_TABLE (`day`) SELECT DISTINCT DATE(`time`) FROM $TABLE_DATA WHERE import='$logFile'})
		or die("Could not execute statement to insert day into $DAYS_TO_COUNT_TABLE for $logFile: " . $DBH->errstr);	
	
	# move analyzed file
	@ARGS = ("/bin/mv", "$DATA_PATH$fileName", "$IMPORTED_LOGS_FOLDER$fileName");
		system(@ARGS) == 0
	or die "Moving file failed from $DATA_PATH$fileName to $IMPORTED_LOGS_FOLDER$fileName: $?";
	 
	##
	# uncomment to test one file
	#exit;

		
}




#########################################################
# ENDING
#########################################################

# close db and result File
$DBH->disconnect();
close (RES);

print"\n================================================\n";
print" LOGIMPORT.PL COMPLETE";
print"\n================================================\n";

	
# continue with analyzing data for direction pairs
my @args = ( "/usr/bin/perl -I$SCRIPT_PATH " . $SCRIPT_PATH."searchdir.pl");
system(@args) == 0 	
	or die "system @args failed: $?";
exit;



#########################################################
# SUBS
#########################################################

##
# kind of main loop for one file
 sub File {
 
 	my ($logFile, $fileName)  = @_;
	
	#################################
	# open LOGFILE for read and clean it from silly windows style line feeds (^M)
	#
	#print "reading file: $logFile\n";
	open(LOG, "< $DATA_PATH$fileName") or die("Can't open file $DATA_PATH$fileName: $!");
	open(LOG_CLEAN, "+> $DATA_PATH$fileName.tmp") or die("Can't open file $DATA_PATH$fileName.tmp: $!");
	
	
	while (my $line = <LOG>) {
		
		$line =~ s/^\s+//g;	
		
		if($line =~ m/\x0D/) {
			chomp($line);
			$line =~ s/\x0D/\n/g;	
		}
		
		print LOG_CLEAN $line;
	}
	
	close(LOG_CLEAN);
	close(LOG);
	
	open(LOG_CLEAN, "< $DATA_PATH$fileName.tmp") or die("Can't open file $DATA_PATH$fileName.tmp: $!");

	
	rename("$DATA_PATH$fileName.tmp", "$DATA_PATH$fileName");
	open(LOG, "< $DATA_PATH$fileName") or die("Can't open file $DATA_PATH$fileName: $!");
	#################################
	
	# reading in LOGFILE and put the data into a table named after the LOGFILE time info
	
	##
	# preparing mysql insert
	my $sth = $DBH->prepare("INSERT INTO `$TABLE_DATA` (time, millisec, ant, rfid, import) VALUES(?,?,?,?,?)")
		or die("Could not prepare insert statement: " . $DBH->errstr);
	my $sthRfid = $DBH->prepare("INSERT INTO $TABLE_RFIDS (id,i) VALUES(?, 0)")
		or die("Could not prepare update statement: " . $DBH->errstr);	

	##
	# when the file is exported from excel it has ^M as the newline character ... replace it with a normal newline
	
	my $line_count = 0;
	my $day = '';
	while (my $lineLog = <LOG>) {
		
		$line_count++;
		
		next if($lineLog =~ m/\t0\t?$/);		# get rid of the size 0 nonsense data (log files with tabs as delimiter)
		next if($lineLog =~ m/\s+0;\s*$/);		# get rid of the size 0 nonsense data (log files with spaces as delimiter)
		
		# inform user
		(($DATACOUNT % 100) == 0) ? print "\n[$DATACOUNT] lines read\t" : print "."; 
		(($DATACOUNT % 100) == 0) ? printf RES "\n[$DATACOUNT] lines read\t" : printf RES "."; 
		
		##
		# prepare the data
		my ($datetime, $millisec, $ant, $rfid) = LineManip($lineLog);
		if(defined $datetime) {
			my @records = ($datetime, $millisec, $ant, $rfid, $logFile);
				
			##
			# insert data into data table	
			$sth->execute(@records)
				or die("Could not insert into $TABLE_DATA: " . $DBH->errstr );	
				
			##
			# update rfid in rfids table
			$sthRfid->execute($records[3])
				or die("Could not update rfid table: " . $DBH->errstr );
		} else {
			print"\n[$line_count] WARNING: Skipping the line. Maybe malformed: $lineLog\n";
			printf RES "\n[$line_count] WARNING: Skipping the line. Maybe malformed: $lineLog\n";

		}
	
		
		$DATACOUNT++;
		
	}

	close(LOG);
	print"\nread: [$fileName]\t  data sets: $DATACOUNT\n";
	printf RES "\n====================\n";
	printf RES "Data sets:\t$DATACOUNT\n";
	printf RES "======================\n";
	$DATACOUNT = 0;
	
 }

##
# Checks if this file has already been imported
sub CheckImport {

	my $fileName = shift;

	my $checkSQL = qq{SELECT count(logfile) FROM $TABLE_LOGS WHERE logfile='$fileName'};
	my $check = $DBH->selectrow_array($checkSQL,undef);
	
	if (($DBH->selectrow_array($checkSQL,undef)) == 1) { 	# remove the file from the data directory if it 
		@ARGS = ("rm", $DATA_PATH.$fileName);					# has already been imported
		system(@ARGS) == 0 or die "system @ARGS failed: $?";
		print"\nERROR: attempt to reimport  [$fileName ]. The file has been deleted from $DATA_PATH\n";
		return 1;
	} else {
		return 0;
	}
	
	return ();

}

##
# Manipulate the data line to get the data we need and format it so that we can put it in the database table
sub LineManip{

	my $logLine = shift;
	
	# get values
	$_ = $logLine;
	
	#
	# some regex to get the values
	my ($datetime,$millisec,$deli1, $ant, $deli2, $deli3, $rfid) = 
	/.*(\d{4}\-\d{2}\-\d{2}\s+\d{2}:\d{2}:\d{2}):(\d{3});?(\s+|\t)(\S{2,3});?(\s+|\t)\d;?(\s+|\t)(\w{2}\s\w{2}\s\w{2}\s\w{2}\s\w{2}).*/;
	
	##
	# check if values are defined
	my @check_values = ($datetime,$millisec,$deli1, $ant, $deli2, $deli3, $rfid);
	my $check = 1;
	
	foreach my $value(@check_values) {
		if(!defined $value) {
			$check = 0;
			last;
		}
	}
	
	if($check == 0) {
		return(undef, undef, undef, undef);
	} else {
	
		## 
		# get rid of whitespaces in the rfid
		$rfid 	=~ s/\s//g;				
		
		##
		# Some special handling for the badly adressed antennas
		$ant =~ s/1A/16/;
		$ant =~ s/2A/17/;
		
		##
		# Antennas 421 and 423 map to box 13 (Mail from B.Koenig 30.12.2008)
		$ant =~ s/421/131/;
		$ant =~ s/423/133/;
		
		##
		# fromatting antenna
		$ant 	= sprintf('%03s', $ant);
		
		##
		# fromatting millisec
		$millisec 	= sprintf('%03s', $millisec);
		
		return($datetime, $millisec, $ant, $rfid);
	}
	
}