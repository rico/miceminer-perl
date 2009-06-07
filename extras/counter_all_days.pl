#!/usr/bin/perl -w

##############################################################################
# This scripts updates the box and ant tables
# - data count for each antenna and box
# - the time with the last dataset (if older then 1 Week, maybe defect)
#
# rleuthold@access.ch // 25.9.2007
##############################################################################

use lib '/sw/lib/perl5/5.8.6/darwin-thread-multi-2level';

use DBI;
use Data::Dumper;

##############################################################################
# SET THIS !!!!!!!!!!!!!
##############################################################################
my $TABLE_DATA			= "data";
my $TABLE_RFIDS			= "rfid";
my $TABLE_RFIDS_COUNT	= "rfid_count";
my $TABLE_DIR			= "dir";
my $TABLE_RES			= "res";
my $TABLE_BOX			= "box";
my $TABLE_BOX_COUNT		= "box_count";
my $TABLE_ANT			= "ant";
my $TABLE_ANT_COUNT		= "ant_count";
##############################################################################


########################################################################
# open db connection
my $DBH = DBI->connect('DBI:mysql:micedata;mysql_socket=/var/mysql/mysql.sock', 'importer', 'wasser',
					{ RaiseError => 0, AutoCommit => 0 })
	           or die "Could not connect to database: $DBI::errstr";
	           
#my $DBH = DBI->connect('DBI:mysql:micedata:aibiria.com:3306;mysql_read_default_file=../conf/dblogin.conf',undef, undef,
#					{ RaiseError => 0, AutoCommit => 0 }) 
#				or die "Could not connect to database: $DBI::errstr";

########################################################################


##############################################################################
# MAIN
##############################################################################	

##
# Get all days in the database
my $DAYS_IN_DB = $DBH->selectcol_arrayref("SELECT DISTINCT DATE(box_out) FROM $TABLE_RES ORDER BY box_out")
		or die("Could not execute statement to get days in database: " . $DBH->errstr);	
		
my $DAYS_RFID_COUNTED = $DBH->selectcol_arrayref("SELECT DISTINCT day FROM $TABLE_BOX_COUNT ORDER BY day")
		or die("Could not execute statement to get days in $TABLE_RFIDS_COUNT: " . $DBH->errstr);	
		
# print " => \$DAYS_IN_DB: " . Dumper($DAYS_IN_DB) ."\n";
# print " => \$DAYS_RFID_COUNTED: " . Dumper($DAYS_RFID_COUNTED) ."\n";

##
# Finding the days which are not already counted (array difference)
my @DAYS_TO_COUNT;
my %days = ();

foreach my $day (@$DAYS_IN_DB, @$DAYS_RFID_COUNTED) {
	$days{$day}++;
}

foreach $day (keys %days) {

	if($days{$day} == 1) {
		push(@DAYS_TO_COUNT, $day);
	}

}

print "------------------------------------\n";		
print "Counting for the following days:\n\n\t" . join("\n\t",@DAYS_TO_COUNT) . "\n";
print "------------------------------------\n";		
		
foreach my $DAY (@DAYS_TO_COUNT) {

	print "------------------------------------\n";		
	print "Counting for date: " . $DAY . "\n";
	print "------------------------------------\n";		

	##
	# Counting antenna data
	&CountAnts($DAY);
	
	##
	# Counting boxes data
	&CountBoxes($DAY);
	
	##
	# Counting rfid data
	&CountRfids($DAY);
}

print "\n--------------------------\n";
print "Count complete\n";
print "--------------------------\n";

$DBH->disconnect();

##############################################################################
# SUBS
##############################################################################

# ANTENNAS
##############################################################################	

sub CountAnts {

	my $day = shift;
	
	##
	# insert ant data
	
	# Searching for ant ids in the data and insert them in the ant table if they are not already there
	my $INS_ANT	= $DBH->prepare("INSERT IGNORE ant(id) SELECT DISTINCT(ant) FROM `data`") 
			or die("Could not prepare statement to insert ants: " . $DBH->errstr);	
	$INS_ANT->execute()
			or die("Could not execute statement to insert ants: " . $DBH->errstr);
	
	##
	# counting ant data
	
	# Preparing statements
	my $COUNT_ANT_DATA 	= $DBH->prepare(qq{UPDATE $TABLE_ANT SET data_count = (SELECT COUNT(id) FROM $TABLE_DATA WHERE ant= ?) WHERE id = ?})
		or die("Could not prepare statement to update ant count: " . $DBH->errstr);
 
	my $LAST_ANT_DATA 	= $DBH->prepare(qq{UPDATE $TABLE_ANT SET last = ( SELECT MAX(time) FROM $TABLE_DATA WHERE ant= ?) WHERE id = ?})
		or die("Could not prepare statement to update ant last: " . $DBH->errstr);		
		
	my $COUNT_ANT_DATA_DAILY = $DBH->prepare(qq{INSERT INTO $TABLE_ANT_COUNT (day,id, data_count, dir_count, res_count) VALUES (
											'$day',?,
											(SELECT COUNT(id) FROM $TABLE_DATA WHERE ant = ? AND DATE(time) = '$day'), 
											(SELECT COUNT(id) FROM $TABLE_DIR WHERE LEFT(box,2) = LEFT(?,2) AND DATE(time) = '$day'), 
											(SELECT COUNT(id) FROM $TABLE_RES WHERE LEFT(box,2) = LEFT(?,2) AND DATE(box_out) = '$day')
										)})
		or die("Could not prepare statement to insert ant counts in $TABLE_ANT_COUNT: " . $DBH->errstr);					
		
	#my $UPDATE_ANT_COUNT_YES = $DBH->prepare(qq{UPDATE $TABLE_ANT SET counted = 1 WHERE id = ? })
	#		or die("Could not prepare statement to update ant count indicator in $TABLE_ANT: " . $DBH->errstr);					
		
				
		
	my @ANTS = map {$_->[0]} @{$DBH->selectall_arrayref("SELECT id FROM $TABLE_ANT")};		
	
	print "\nupdating antenna data count\n\n";
	
	foreach my $ant (@ANTS) {
	
		print"[$day]\tupdating count data for ant: $ant ...\n";
		# getting data count sum
		$COUNT_ANT_DATA->execute($ant, $ant)
			or die("Could not execute statement to count ant data: " . $DBH->errstr);	
			
		# check for the last dataset
		$LAST_ANT_DATA->execute($ant, $ant)
			or die("Could not execute statement to update ant last: " . $DBH->errstr);		
		
		# daily count 
		$COUNT_ANT_DATA_DAILY->execute($ant,$ant,$ant,$ant)
			or die("Could not execute statement for daily count for ants: " . $DBH->errstr);
 	}	
}

# BOXES
##############################################################################	

sub CountBoxes {

	my $day = shift;
		
	##
	# insert BOX data
	my $INS_BOX	= $DBH->prepare("INSERT IGNORE box(id) SELECT DISTINCT LEFT(id, 2) from $TABLE_ANT") 
			or die("Could not prepare statement to insert boxes: " . $DBH->errstr);
	$INS_BOX->execute
			or die("Could not execute statement to insert boxes: " . $DBH->errstr);	
	
	
	my $COUNT_BOX_DATA 	= $DBH->prepare(qq{update box set data_count = (SELECT COUNT(id) FROM $TABLE_DATA WHERE LEFT(ant,2) = ?) WHERE id = ?})
			or die("Could not prepare statement to update box count: " . $DBH->errstr);		
			
	my $LAST_BOX_DATA 	= $DBH->prepare(qq{UPDATE box set last = (SELECT MAX(last) FROM $TABLE_ANT WHERE LEFT(id, 2) = ?) WHERE id = ?})
			or die("Could not prepare statement to update box last: " . $DBH->errstr);
			
	my $COUNT_BOX_DATA_DAILY = $DBH->prepare(qq{INSERT INTO $TABLE_BOX_COUNT (day,id, data_count, dir_count, res_count) VALUES (
												'$day',?,
												(SELECT COUNT(id) FROM $TABLE_DATA WHERE LEFT(ant,2) = ? AND DATE(time) = '$day'), 
												(SELECT COUNT(id) FROM $TABLE_DIR WHERE box = ? AND DATE(time) = '$day'), 
												(SELECT COUNT(id) FROM $TABLE_RES WHERE box = ? AND DATE(box_out) = '$day')
											)})
			or die("Could not prepare statement to insert box counts in $TABLE_BOX_COUNT: " . $DBH->errstr);				
			
	my @BOXES = map {$_->[0]} @{$DBH->selectall_arrayref("SELECT id FROM $TABLE_BOX")};
	
	print "\nupdating boxes data count\n\n";
			
	foreach my $box (@BOXES) {
	
		print"[$day]\tupdating count data for box: $box ...\n";
		$COUNT_BOX_DATA->execute($box, $box)
			or die("Could not execute statement to count box data: " . $DBH->errstr);	
			
		$LAST_BOX_DATA->execute($box, $box)
			or die("Could not execute statement to update box last: " . $DBH->errstr);				
			
		# daily count 
		$COUNT_BOX_DATA_DAILY->execute($box,$box,$box,$box)
			or die("Could not execute statement for daily count for boxes: " . $DBH->errstr);
	}	
}

# RFIDS
##############################################################################	

sub CountRfids {
	
	my $day = shift;
	
	my $COUNT_RFID_DATA = $DBH->prepare(qq{update $TABLE_RFIDS set 
			data_count = (SELECT COUNT(id) FROM $TABLE_DATA WHERE rfid = ?),
			dir_count = (SELECT COUNT(id) FROM $TABLE_DIR WHERE rfid = ?),
			res_count = (SELECT COUNT(id) FROM $TABLE_RES WHERE rfid = ?)
			WHERE id = ?})
			or die("Could not prepare statement to update rfid counts: " . $DBH->errstr);		
			
	my $LAST_RFID_DATA 	= $DBH->prepare(qq{UPDATE $TABLE_RFIDS set last = (SELECT MAX(time) FROM $TABLE_DATA WHERE rfid= ?)  WHERE id = ?})
			or die("Could not prepare statement to update rfid last: " . $DBH->errstr);
			
	my $COUNT_RFID_DATA_DAILY = $DBH->prepare(qq{INSERT INTO $TABLE_RFIDS_COUNT (day,id, data_count, dir_count, res_count) VALUES (
												'$day',?,
												(SELECT COUNT(id) FROM $TABLE_DATA WHERE rfid = ? AND DATE(time) = '$day'), 
												(SELECT COUNT(id) FROM $TABLE_DIR WHERE rfid = ? AND DATE(time) = '$day'), 
												(SELECT COUNT(id) FROM $TABLE_RES WHERE rfid = ? AND DATE(box_out) = '$day')
											)})
			or die("Could not prepare statement to insert rfid counts in $TABLE_BOX_COUNT: " . $DBH->errstr);					
			
	my @RFIDS = map {$_->[0]} @{$DBH->selectall_arrayref("SELECT DISTINCT id FROM $TABLE_RFIDS")};
			
	foreach my $rfid (@RFIDS) {
	
		print"[$day]\tupdating data for rfid: $rfid ...\n";
		
		$COUNT_RFID_DATA->execute($rfid, $rfid, $rfid, $rfid)
			or die("Could not execute statement to count rfid data: " . $DBH->errstr);	
			
		$LAST_RFID_DATA->execute($rfid, $rfid)
			or die("Could not execute statement to update rfid last: " . $DBH->errstr);				
			
		# daily count 
		$COUNT_RFID_DATA_DAILY->execute($rfid,$rfid,$rfid,$rfid)
			or die("Could not execute statement for daily count for rfids: " . $DBH->errstr);			
		
	}
}

