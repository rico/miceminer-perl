#!/usr/bin/perl -w

##############################################################################
# Searches for dirction results in the raw data created by 'logimport.pl'
# - Step 2 of importing the data.
#
# Continues with 'searchres.pl' when finished.
#
# rleuthold@access.ch - 8.1.2009
##############################################################################

use strict;
use DBI;
use Cwd;
use Fcntl;
use Date::Calc qw(:all);
use Data::Dumper;

use lib 'lib';
use lib::DBHandler;
use lib::XMLPaths;
use lib::DBTables;

##############################################################################
##
# Global variables
my $DBH; 
my $TABLES		= DBTables->new();
my $PATHS		= XMLPaths->new();
my $PERLCONFIG 	= PerlConfig->new();
# Paths / directories
my $DATA_PATH 	= $PATHS->get_path('data');
my $SUBFOLDER 	= $PATHS->get_path('imported');

my $SCRIPT_PATH	= $PERLCONFIG->get_scriptsfolder();

# database tables
my $TABLE_DATA	= $TABLES->get_table_name('data');
my $TABLE_RFIDS	= $TABLES->get_table_name('rfids');
my $TABLE_DIR   = $TABLES->get_table_name('direction_results');
my $DAYS_TO_COUNT_TABLE = $TABLES->get_days_to_count_table();

my $INTERVAL    = $PERLCONFIG->get_antennainterval(); 	# Max time in seconds between the two antenna readings 
														# at a box to form a valid direction results
my $RFIDDATA;
my @RFIDDATASORTEDKEYS;	# sort

my $PRINTF = "%-12s%-7s%-22s%-4s\n";
my $PRINTFSEP = "-----------+------+---------------------+---+\n";

##############################################################################
##
# some counters
my $RESCOUNT   = 0;
my $NORESCOUNT = 0;
my $IDCOUNT    = 0;
my $RFIDCOUNT  = 0;
my $RFIDDATAKEYCOUNT = 0;
my $SAME_TIME = 0;

####################################
# PREAMBLE
####################################
print"\n================================================\n";
print"STARTING SEARCHDIR.PL\n\n";
print "Symbols:\n";
print "\t+ => direction result (1-3) -> (3-1)\n";
print "\t- => no result\n";
print"\n================================================\n";
################################
# set up results file
my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) =
  localtime time;
my $dayDate = $mday . "_" . ( $mon + 1 ) . "_" . ( $year + 1900 ) . "/";
my $DAY_FOLDER = $DATA_PATH . $SUBFOLDER . $dayDate;
$DAY_FOLDER = $SUBFOLDER . $dayDate;
mkdir( $DAY_FOLDER, 0771 ) unless ( -d $DAY_FOLDER );
my $filename = $DAY_FOLDER . "dir_data\.txt";    # the filename with some kind of timestamp
sysopen( RES, $filename, O_CREAT | O_WRONLY, 0755 )
  or die("Can't open result file $filename: $!");
####################################
# open db connection
$DBH = DBHandler->new()->connect();
######
# specifying/preparing the sql statements which are used so many times (for performance reasons)
my $SELRFIDSTH = $DBH->prepare( qq{SELECT id,time, (UNIX_TIMESTAMP(time) * 1000 + millisec) as unix_time_milli, ant, import FROM `$TABLE_DATA` WHERE rfid= ? AND i='0'})
  or die( "Could not prepare statement to get rfid contents: " . $DBH->errstr );
  
my $INSRESSTH = $DBH->prepare(qq{INSERT INTO `$TABLE_DIR` (rfid,time, box, dir, outerdataid, innerdataid) VALUES( ?, ?, ?, ?, ?, ? )})
  or die( "Could not prepare statement to insert the res items: " . $DBH->errstr );
  
my $UPDATERESSTH = $DBH->prepare(qq{UPDATE `$TABLE_DATA` set i=2, dir_id = ? WHERE id IN( ?, ? )})
  or die( "Could not prepare statement to update the data table items: ". $DBH->errstr );
  
my $UPDATENORESSTH = $DBH->prepare(qq{UPDATE `$TABLE_DATA` SET i=1 WHERE id= ?})
  or die( "Could not prepare statement to update the res items: " . $DBH->errstr );							
  
##############################################################
# MAIN
##############################################################

##
#Create 'temporary' table to store the days which have to be counted in the counter.pl script.
$DBH->do(qq{CREATE TABLE IF NOT EXISTS `$DAYS_TO_COUNT_TABLE` (`day` date NOT NULL, PRIMARY KEY (`day`) )}) 
	|| die ("Could not create table '$DAYS_TO_COUNT_TABLE': " . $DBH->errstr);
$DBH->do(qq{INSERT IGNORE INTO $DAYS_TO_COUNT_TABLE (`day`) SELECT DISTINCT DATE(`time`) FROM $TABLE_DATA WHERE i='0'}) 
	|| die ("Could not insert days to count into $DAYS_TO_COUNT_TABLE: " . $DBH->errstr);
	
my @RFIDS = map { $_->[0] } @{ $DBH->selectall_arrayref("SELECT id FROM $TABLE_RFIDS WHERE i='0'") };


##
# print header text to the result file
printf RES ($PRINTF,"rfid","box","time","dir");
printf RES $PRINTFSEP;

##
# main loop to search every rfid table
foreach my $rfid (@RFIDS) {    # search each rfid
	$RFIDCOUNT++;
	print "[rfid $RFIDCOUNT of " . scalar(@RFIDS). "] => [$rfid]\t";

	#print Dumper($_)."\n";
	##
	# get all the data for this rfid
	# this is the main hash we play with
	$RFIDDATA = {};
	$RFIDDATA = &SelDBtbl($rfid);
	
	##
	# Jump to next rfid if we have no data to search.
	if(scalar keys(%$RFIDDATA) == 0) {
		print "\n\t(no data)\n";
		##
		# update the info in the rfid table for the direction analyze
		$DBH->do(qq{UPDATE $TABLE_RFIDS SET dir=NOW(), i=2 WHERE id='$rfid'})
	  		or die( "Could not clear Table $TABLE_RFIDS: " . $DBH->errstr );
		next;
	}
	
	##
	# Create an array with the id's sorted by time ascending
	@RFIDDATASORTEDKEYS = ();
	foreach my $rfiddatakey( sort{ $RFIDDATA->{$a}->{'unix_time_milli'} <=> $RFIDDATA->{$b}->{'unix_time_milli'} } keys(%$RFIDDATA) ) {
		push( @RFIDDATASORTEDKEYS, $rfiddatakey); 
	}
	
	##
	# main loop to search within rfid tables
	$RFIDDATAKEYCOUNT = 0;
	print "\n\t";
	
	while ( @RFIDDATASORTEDKEYS > 0) {    # search while we have entries in the table
		$RFIDDATAKEYCOUNT++;
		my $res_feedback = &GetRes( $rfid, shift(@RFIDDATASORTEDKEYS), \@RFIDDATASORTEDKEYS,$RFIDDATA );
		printf RES $res_feedback;    # get results;
	}
	
	
	print " [" . $RFIDDATAKEYCOUNT . "]\n";
	
	##
	# update the info in the rfid table for the direction analyze
	$DBH->do(qq{UPDATE $TABLE_RFIDS SET dir=NOW(), i=2 WHERE id='$rfid'})
	  or die( "Could not clear Table $TABLE_RFIDS: " . $DBH->errstr );
	##
	# uncomment next line to test only one rfid
	exit;
}
#########################################################
# Ending
#########################################################
# close db and result File
$DBH->disconnect();

my $summary = qq{
================================================
rfids:\t\t$RFIDCOUNT
Data sets:\t$IDCOUNT
Results:\t$RESCOUNT
No Result:\t$NORESCOUNT
================================================
};
printf RES $summary;
close(RES);

print $summary;

print"\n================================================\n";
print"SEARCHDIR.PL COMPLETE";
print"\n================================================\n";

##
# continue
#my @args = ( $SCRIPT_PATH . "searchres.pl" );
#system(@args) == 0
#  or die "system @args failed: $?";
exit;
##############################################################
# SUBS
##############################################################
##
# select id time and box from the rfid table
sub SelDBtbl {
	my $rfid     = shift;
	my $rfidData = {};
	my @fields   = (qw(id time unix_time_milli ant import));
	$SELRFIDSTH->execute($rfid);
	my %rec = ();
	$SELRFIDSTH->bind_columns( map { \$rec{$_} } @fields );
	while ( $SELRFIDSTH->fetchrow_arrayref ) {
		$rfidData->{ $rec{'id'} } = {%rec};
	}
	return ($rfidData);
}
##
# check for the timespan pairs
sub GetRes {
	my ($rfid,$toCheckId,$sorted_keys, $rfidData)  = @_;
	
	my $toCheckItem = $rfidData->{$toCheckId};
	my $toCheckInterval = $toCheckItem->{'unix_time_milli'} + ($INTERVAL * 1000);
	
	my $i = 0;
	
	##
	# Search for a matching data item in TIME.
	while(defined(@{$sorted_keys}[$i]) && $rfidData->{@$sorted_keys[$i]}->{'unix_time_milli'} <= $toCheckInterval) { 
		
		my $possibleMatch = $rfidData->{@$sorted_keys[$i]};
		if($toCheckItem->{'unix_time_milli'} == $possibleMatch->{'unix_time_milli'}) {
			$SAME_TIME++;
			return ( &NoRes($toCheckId) );
		} elsif ($toCheckItem->{'unix_time_milli'} > $possibleMatch->{'unix_time_milli'}) {
			die("Next item is before the item to check: tocheckItem => " . Dumper($toCheckItem) ." possibleMatch => " . Dumper($possibleMatch) . "\n");	
		}

		##
		# Search for matching ANTENNA data item. 
		if( &CheckAnt( $toCheckItem, $possibleMatch) ) {
			##
			# We found a matching ANTENNA (and time)
			
			# Delete the matching key from the @RFIDDATASORTEDKEYS
			@RFIDDATASORTEDKEYS = @{&DelItemFromArray($i, \@RFIDDATASORTEDKEYS)};
			
			$IDCOUNT += 2;
			(($RFIDDATAKEYCOUNT % 100) == 0 && $RFIDDATAKEYCOUNT != 0) ? print "+ [$RFIDDATAKEYCOUNT]\n\t" : print "+";
			return ( &Res($rfid, $toCheckItem,  $possibleMatch) );    # handle the results
		
		}
		
		$i++;
	}
		
	##
	# No matching data found - no result	
	(($RFIDDATAKEYCOUNT % 100) == 0 && $RFIDDATAKEYCOUNT != 0) ? print "- [$RFIDDATAKEYCOUNT]\n\t" : print "-";
	$IDCOUNT++;
	return ( &NoRes($toCheckId) );

}
##
# Check for matching antenna.
#
# returns 1 for a match, else 0
sub CheckAnt {
	my ( $toCheck, $timePoss ) = @_;
	
#	print "CheckAnt:\n";
#	print "\$toCheck => " . Dumper($toCheck) ."\n";
#	print "\$timePoss => " . Dumper($timePoss) ."\n";
#	sleep(10);
	
	abs($toCheck->{'ant'} -  $timePoss->{'ant'}) == 2 ? return 1 : return 0;
	
}

##
# Get Direction for a result.
#
# returns 'out' or 'in'
sub GetDir {
	my ( $dataset_one, $dataset_two) = @_;

	$dataset_one->{'ant'} -  $dataset_two->{'ant'} == 2 ? return 'out' : return 'in';
}

##
# insert result into the $TABLE_DIR table and update the indicators in the rfid table
sub Res {
	my ( $rfid, $dataset_one, $dataset_two) = @_;
	
	my $direction =  &GetDir($dataset_one, $dataset_two);
	
	
	# ids/time
	##
	my $box = substr( $dataset_one->{'ant'}, 0, 2 );
	my ( $outerid, $innerid, $import, $time );
	if ( $direction eq 'in' ) {
		$innerid = $dataset_two->{'id'};
		$outerid = $dataset_one->{'id'};
		$time = $dataset_one->{'time'};
		
	} else {
		$innerid = $dataset_one->{'id'};
		$outerid = $dataset_two->{'id'};
		$time = $dataset_two->{'time'};
	}
	
	##
	# insert result into direction result table
	$INSRESSTH->execute($rfid, $time, $box, $direction, $outerid, $innerid)
	  or die( "Could not insert into $TABLE_DIR: " . $DBH->errstr );
	  
	##
	# get insert id
	my $dir_id = $DBH->{ q{mysql_insertid} };
	  
	##
	# update the data table - more exactly: set the indicator (i) to 2 cause these two items build a direction pair together
	$UPDATERESSTH->execute($dir_id, $innerid, $outerid)
	  or die( "Could not update $TABLE_DATA: " . $DBH->errstr );
	  
	##
	$RESCOUNT += 2;

	return sprintf($PRINTF, $rfid, $box, $time, $direction);
}
##
# update the indicator (i) in the rfid table
sub NoRes {
	my $noResId = shift;

	$UPDATENORESSTH->execute($noResId)
	  or die "Couldn't update table $TABLE_DATA : " . $DBH->errstr;
	  
	##
	$NORESCOUNT++;
	return ("");
}
##
# If we have multiple possible results, get the one with the lowest millisec value.
sub MultiRes {
	my $multiRes = shift;

	my $lowest_val_key = (sort {$multiRes->{$a}->{'millisec'} <=> $multiRes->{$b}->{'millisec'}} keys %$multiRes)[0];
	return ( $multiRes->{$lowest_val_key});
	
}
##
# Delete the passed index from the passed array and return the new array.
sub DelItemFromArray {

	my ($index, $arr) = @_;
	
	splice(@$arr, $index, 1);		
	return $arr;
}