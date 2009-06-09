#!/usr/bin/perl -w

##############################################################################
# Searches for results - Step 3 of importing the data.
#
# Continues with 'counter.pl' when finished.
#
# rleuthold@access.ch - 8.1.2009
##############################################################################

use strict;
use DBI;
use Cwd;
use Fcntl;
use Date::Calc qw(Delta_YMDHMS);
use DateTime::Format::Strptime;
use Data::Dumper;

use lib 'lib';
use lib::DBHandler;
use lib::XMLPaths;
use lib::DBTables;

##
# Global variables
my $DBH; 
my $TABLES		= DBTables->new();
my $PATHS		= XMLPaths->new();
my $PERLCONFIG	= PerlConfig->new();

# Paths / directories
my $DATA_PATH 	= $PATHS->get_path('data');
my $SUBFOLDER 	= $PATHS->get_path('imported');

my $SCRIPT_PATH	= $PERLCONFIG->get_scriptsfolder();

# database tables
my $TABLE_DATA	= $TABLES->get_table_name('data');
my $TABLE_RFIDS = $TABLES->get_table_name('rfids');
my $TABLE_DIR	= $TABLES->get_table_name('direction_results');
my $TABLE_RES	= $TABLES->get_table_name('results');

my $DAYS_TO_COUNT_TABLE = $TABLES->get_days_to_count_table();

my $RFIDDIRINDATA = ();
my @RFIDDIRINDATASK = ();
my $RFIDDIROUTDATA = ();
my @RFIDDIROUTDATASK = ();
my $RFIDOUTDATA = ();
my @RFIDOUTDATASK = ();
my @RFIDRESSORTEDSK = ();

# some counters
my $DATACOUNT = 0; # number of (in) datasets
my $RESCOUNT = 0; # number if results  
my $DIRRESCOUNT = 0; # number of direction result 
my $DATARESCOUNT = 0; # number of data results 
my $NORESCOUNT	= 0; # number of in datasets for which no resuult could be found
my $OVERLAPS = 0; # number of overlaps 
my $RFIDCOUNT 	= 0; # number of rfids

#my $BOX_OUT;		# store the time from which we continue to search
my $PRINTF = "%-12s%-7s%-22s%-22s%-9s\n";
my $PRINTFSEP = "-----------+------+---------------------+---------------------+--------+\n";


####################################
# PREAMBLE
####################################
print"\n================================================\n";
print "STARTING SEARCHRES.PL\n\n";
print "Symbols:\n";
print "\t+ => direction result (1-3) -> (3-1)\n";
print "\t& => data result (1-3) -> (1)\n";
print "\t- => no result\n";
print "\to => overlap (no result)\n";
print"\n================================================\n";

################################
# set up results file

##
# where should we write the output 

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime time;
my $dayDate = $mday."_". ($mon+1) ."_". ($year+1900) ."/";
my $DAY_FOLDER = $SUBFOLDER . $dayDate;

##
# Try to create folders if they don't exist
mkdir($SUBFOLDER, 0771) unless (-d $SUBFOLDER);
mkdir($DAY_FOLDER, 0771) unless (-d $DAY_FOLDER);

my $filename = $DAY_FOLDER. "res_data\.txt";		# the filename with some kind of timestamp
sysopen (RES, $filename, O_CREAT |O_WRONLY, 0755)  or die("Can't open result file : $!");


####################################
# open db connection
$DBH = DBHandler->new()->connect();
	
######
# specifying/preparing the sql statements which are used many times (for performance reasons)

#my $LASTOUTRFID = $DBH->prepare(qq{SELECT MAX(box_out) FROM `$TABLE_RES` WHEERE rfid = ?}) 
#		or die("Could not prepare statement to last box out: " . $DBH->errstr);	

my $RFIDDIRDATAINSTH 	= $DBH->prepare(qq{SELECT id, rfid, time, UNIX_TIMESTAMP(time) AS unix_time, box, dir, outerdataid, innerdataid FROM `$TABLE_DIR` WHERE rfid= ? AND i=0 AND dir='in' AND `time` > ?})
		or die( "Could not prepare statement to get rfid dir results: " . $DBH->errstr);
			
my $RFIDDIRDATAOUTSTH 	= $DBH->prepare(qq{SELECT id, time, UNIX_TIMESTAMP(time) AS unix_time, box, dir, outerdataid, innerdataid FROM `$TABLE_DIR` WHERE rfid= ? AND i=0 AND dir = 'out' AND i=0 AND time > ?})
		or die( "Could not prepare statement to get rfid dir results: " . $DBH->errstr);			
		
my $RFIDOUTDATASTH = $DBH->prepare(qq{SELECT id, time, UNIX_TIMESTAMP(time) AS unix_time, LEFT(ant,2) AS box, id AS outerdataid, id AS innerdataid FROM `$TABLE_DATA`  WHERE rfid= ? AND i=1 AND RIGHT(ant,1) = '1' AND time > ?} )		
		or die("Could not prepare statement to get rfid data results: " . $DBH->errstr);	
		
my $INSRESSTH = $DBH->prepare(qq{INSERT INTO `$TABLE_RES` (rfid, box, box_in, box_out, dt, inid, outid, i, nerv_index) VALUES( ?, ?, ?, ?, SEC_TO_TIME(?), ?, ?, ?, ?)})
		or die("Could not prepare statement to insert result: " . $DBH->errstr);
		
my $UPDATEDATATABLE = $DBH->prepare(qq{UPDATE $TABLE_DATA SET i= ? WHERE id IN(?,?,?,?)})
		or die("Could not prepare statement to update $TABLE_DATA: " . $DBH->errstr);		
		
my $UPDATEDIRTABLE = $DBH->prepare(qq{UPDATE $TABLE_DIR SET i= ? WHERE id IN(?,?)})
		or die("Could not prepare statement to update $TABLE_DIR: " . $DBH->errstr);

my $READINGS_DURING_DIRECTION_RESULT = $DBH->prepare(qq{SELECT * FROM $TABLE_DATA WHERE id != ? AND rfid=? AND time > ? AND time < 
	(SELECT time FROM $TABLE_DATA WHERE id = (SELECT innerdataid FROM $TABLE_DIR WHERE id = ?));
})	
	or die("Could not prepare statement to check for other readings in table $TABLE_DATA: " . $DBH->errstr);

my $READINGS_DURING_DATA_RESULT = $DBH->prepare(qq{SELECT * FROM $TABLE_DATA WHERE rfid=? AND time > ? AND time < ?})	
	or die("Could not prepare statement to check for other readings in table $TABLE_DATA: " . $DBH->errstr);
	
my $UPDATEDATARESIDS = $DBH->prepare(qq{UPDATE $TABLE_DATA SET res_id= ? WHERE id IN(?,?,?,?)})
		or die("Could not prepare statement to update $TABLE_DATA: " . $DBH->errstr);
		
my $UPDATEDIRRESIDS = $DBH->prepare(qq{UPDATE $TABLE_DIR SET res_id= ? WHERE id IN(?,?)})
		or die("Could not prepare statement to update $TABLE_DIR: " . $DBH->errstr);
						

##############################################################	           
# MAIN
##############################################################

##
#Create 'temporary' table to store the days which have to be counted in the counter.pl script.
$DBH->do(qq{CREATE TABLE IF NOT EXISTS `$DAYS_TO_COUNT_TABLE` (`day` date NOT NULL, PRIMARY KEY (`day`) )}) 
	|| die ("Could not create table '$DAYS_TO_COUNT_TABLE': " . $DBH->errstr);
$DBH->do(qq{INSERT IGNORE INTO $DAYS_TO_COUNT_TABLE (`day`) SELECT DISTINCT DATE(`time`) FROM $TABLE_DIR WHERE i='0'}) 
	|| die ("Could not insert days to count into $DAYS_TO_COUNT_TABLE: " . $DBH->errstr);	 

##
# getting rfids which are already analyzed for direction
my @RFIDS = map {$_->[0]} @{$DBH->selectall_arrayref("SELECT id FROM $TABLE_RFIDS WHERE i='2'")};

# 

# print header text to the result file
printf RES ( $PRINTF,"rfid","box","box_in","box_out","dt");
printf RES $PRINTFSEP;

# Now search for each rfid
foreach my $rfid (@RFIDS) {

	$RFIDCOUNT++;
	print "[rfid $RFIDCOUNT of " . scalar(@RFIDS). "] => [$rfid]\t";
	
	##
	# get the latest box out from the result table to avoid result overlap
	my $max_time = shift(@{$DBH->selectcol_arrayref(qq{SELECT MAX(box_out) FROM `$TABLE_RES` WHERE rfid = '$rfid'})}) || '0000-00-00 00:00:00';
		
	##
	# Get an array reference with the direction IN data for the rfid, the key is the time in the unix timestamp format
	$RFIDDIRINDATA = SelArrayRef($RFIDDIRDATAINSTH, [qw{id rfid time unix_time box dir outerdataid innerdataid}], [$rfid, $max_time]);
#	$RFIDDIRDATAINSTH->execute($rfid,$max_time);
#	$RFIDDIRINDATA = $RFIDDIRDATAINSTH->fetchall_hashref('id');
	##
	# Jump to next rfid if we have no IN data to search.
	if(scalar keys(%$RFIDDIRINDATA) == 0) {
		print "\n\t(no data)\n";
		##
		# update the rfids table to indicate that this rfid has been searched for in/out pairs
		$DBH->do(qq{UPDATE `$TABLE_RFIDS` set i='3', res=NOW() WHERE id = '$rfid'})
			or die("Could not update $TABLE_RFIDS -> $rfid: " . $DBH->errstr);
		next;
	}
	
	##
	# Sorted keys for the in data
	@RFIDDIRINDATASK = @{&sortedHashKeysArray($RFIDDIRINDATA, 'unix_time')};
	$DATACOUNT += scalar(@RFIDDIRINDATASK);
	
	#
	# Get an array reference with the direction OUT data for the rfid, the key is the time in the unix timestamp format
	$RFIDDIROUTDATA = SelArrayRef($RFIDDIRDATAOUTSTH, [qw{id time unix_time box dir outerdataid innerdataid}], [$rfid, $max_time]);
	
	##
	# Sorted keys for the OUT data
	@RFIDDIROUTDATASK = @{&sortedHashKeysArray($RFIDDIROUTDATA, 'unix_time')};
	
	##
	# Get an array reference with the OUT data for the rfid, the key is the time in the unix timestamp format
	$RFIDOUTDATA = SelArrayRef($RFIDOUTDATASTH, [qw{id time unix_time box outerdataid innerdataid}], [$rfid, $max_time]);
	
	##
	# Sorted keys for the data data
	@RFIDOUTDATASK = @{&sortedHashKeysArray($RFIDOUTDATA, 'unix_time')}; 
	
	##
	# main loop
	my $rfidresults = {};
	my $rfid_no_results = {};
	@RFIDRESSORTEDSK = ();
	
	##
	# Search for result pair
	my $check_id_count = 0;
	print "\n\t=> Getting results:\n\t";
	my $rfid_data_count = scalar(@RFIDDIRINDATASK);
	
	##
	# Looping through the IN direction data for the rfid and search for matching OUT data.
	while (scalar(@RFIDDIRINDATASK) > 0 ) {
		
		##
		# Getting matching OUT result for the IN result defined by the $toCheckId.
		my $toCheckId = shift(@RFIDDIRINDATASK);
		
		my $result = &SearchMatchOut($rfid, $RFIDDIRINDATA->{$toCheckId});
		
		##
		# SearchMatchOut returns a hash reference with an 'in' and an out 'key' with the respective values.
		# If we have no 'out' key, no matching out result could be found.
		if( defined $result->{'out'} )  {
			
			$result->{'result'} = 1;
			$rfidresults->{$toCheckId} =  $result; # add the result to the hash with the results for this rfid
			
			##
			# Delete `used` direction 'out' key from the array with the sorted keys for the result type.
			if($result->{'typ'} == 3) { # direction result
				@RFIDDIROUTDATASK = @{&DelItemFromArray($result->{'out'}->{'index'}, \@RFIDDIROUTDATASK)};
			} else { # data result
				@RFIDOUTDATASK = @{&DelItemFromArray($result->{'out'}->{'index'},\@RFIDOUTDATASK)}; 
			} 
			
			push(@RFIDRESSORTEDSK, $toCheckId);
			
		##
		# No matching 'out' data could be found for this id
		} else {
			$result->{'result'} = 0;
			$rfidresults->{$toCheckId} = $result;
		}
		
		
		$check_id_count++;
		(($check_id_count % 100) == 0) ? print ". [$check_id_count]\n\t" : print ".";
		
		##
		# uncomment next line to check only one id.
		#exit;
	}
	
	print " [$rfid_data_count]";
	
	##
	# After finding the result pairs we check for overlapping (time) results
	
	##
	# Loop through results and check for overlaps.
	$check_id_count = 0;
	print "\n\t=> Solving overlap conflicts (+ = result overlap, * = dataset overlap, & = nervous, - = no conflict):\n\t";
	
	while (scalar(@RFIDRESSORTEDSK) > 0) {
		
		my $res_sorted_key = shift(@RFIDRESSORTEDSK);
		my $res_sorted_key_overlap = searchforOverlap($res_sorted_key, $rfidresults);
		
		$check_id_count++;
		
		##
		# resolve the overlap conflict (mark the overlaps)
		if ( scalar (@$res_sorted_key_overlap)  >  0 ) {			
			(($check_id_count % 100) == 0) ? print "+ [$check_id_count]\n\t" : print "+";
			
			##		
			# case 1: The result in question overlaps many others.
			#
			# The chance of one `wrong` result (missed antenna reading) is
			# higher the chance of multiple `false` readings building results together.
			# So we mark the result (the one in question) which overlaps the other ones. 
			if( scalar(@$res_sorted_key_overlap) > 1) {
				$rfidresults->{ $res_sorted_key }->{'overlap'} = 1;
				
			##	
			# case 2: The result in question overlaps just one other.
			#
			# We skip the reuslt we are working on and keep the other
			#
			} else {
				$rfidresults->{ $res_sorted_key }->{'overlap'}	= 1;
				#$rfidresults->{(@{$res_sorted_key_overlap}[0])}->{'overlap'} = 1;
			}
			 
		##
		# No result overlapping detectected	
		} else {
			
			##
			# Check for DATASET overlapping
			$rfidresults->{ $res_sorted_key }->{'overlap'} = 0;
			$rfidresults->{ $res_sorted_key }->{'nerv_index'} = 0;
			my $dataset_overlaps = &searchOtherReadings($res_sorted_key, $rfidresults);
			
			
			# if we have dataset overlapping we check if the overlapped datasets are readings at the inner antenna (3)
			# of the box this result takes place. 
			# If this is the case, this is a 'nervous result', because the mouse is checking the entry tube quite often.
			# The nerv_index value specifies the number of readings at tthe inner antenna 
			# (some kind of measurement of how nervous the mouse is)
			if(keys %$dataset_overlaps) {
			 
				if ( &checkNervousness($res_sorted_key, $rfidresults, $dataset_overlaps) ) {
					(($check_id_count % 100) == 0) ? print "& [$check_id_count]\n\t" : print "&";
					$rfidresults->{ $res_sorted_key }->{'nerv_index'} = scalar(keys %$dataset_overlaps);		
				} else {
					(($check_id_count % 100) == 0) ? print "* [$check_id_count]\n\t" : print "*";
					$rfidresults->{ $res_sorted_key }->{'overlap'} = 1;	
				}
				
			# no dataset overlapping	
			} else {
				(($check_id_count % 100) == 0) ? print "- [$check_id_count]\n\t" : print "-";
				$rfidresults->{ $res_sorted_key }->{'overlap'} = 0;	
			}
		
		}
	}
	
	print " [$check_id_count]";
	
	##
	# Writing results / no results do the database and update the datasets in the
	# data and direction.
	# 	Meaning of i values:
	#		1 => no result found
	# 		3 => part of direction result
	#		4 => part of data result  
	##
	 
	$check_id_count = 0;
	
	print "\n\t=> Writing results to database (+ = result typ 3, & = result typ 4, o = overlap (no result), - = no result):\n\t";
	foreach my $res_key(  keys(%$rfidresults) ) {
		
		##
		# getting 'in' ids to update
		my $res_item = $rfidresults->{$res_key};
		my $inid	= $res_item->{'in'}->{'id'};
		my $inid_innerdataid = $res_item->{'in'}->{'innerdataid'};
		my $inid_outerdatait = $res_item->{'in'}->{'outerdataid'};
		my ($outid, $outid_innerdataid, $outid_outerdatait);
		
		##
		# if an 'out' part is present, get the values
		if( defined $res_item->{'out'} ) {
			$outid	= $res_item->{'out'}->{'id'};
			$outid_innerdataid = $res_item->{'out'}->{'innerdataid'};
			$outid_outerdatait = $res_item->{'out'}->{'outerdataid'};
		}
		
		$check_id_count++;
		
		##
		# items for which no data could be found
		if( !$res_item->{'result'} ) {
			
			(($check_id_count % 100) == 0) ? print "- [$check_id_count]\n\t" : print "-";
			# Updating dataset in the direction table
			&UpdateDirTable( $inid, $inid, 1 );
			
			$NORESCOUNT++;
		
		##
		# results with overlap (not a result)
		} elsif( $res_item->{'overlap'} ) {
			
			(($check_id_count % 100) == 0) ? print "o [$check_id_count]\n\t" : print "o";
			if($res_item->{'typ'} == 3) {
				# Updating datasets in the direction table
				&UpdateDirTable( $inid, $outid, 1);
				
			} else {
				# Updating datasets in the direction table
				&UpdateDirTable( $inid, $inid, 1);
			}
			
			$OVERLAPS++;
			$NORESCOUNT++;
		
		##
		# results without overlap -> insert into result table
		} elsif( $res_item->{'result'} && !$res_item->{'overlap'} ) {
			if($rfidresults->{$res_key}->{'typ'} == 3) {
				(($check_id_count % 100) == 0) ? print "+ [$check_id_count]\n\t" : print "+";
				# Updating datasets in the direction table
				&UpdateDirTable($inid, $outid, 3);
				# Updating datasets in the data table
				&UpdateDataTable($inid_innerdataid,$inid_outerdatait,$outid_innerdataid,$outid_outerdatait, 3);
				#print "\$res_item => " . Dumper($res_item) ."\n";
				
				$DIRRESCOUNT++
				
			} else {
				(($check_id_count % 100) == 0) ? print "& [$check_id_count]\n\t" : print "&";
				# Updating datasets in the direction table
				&UpdateDirTable($inid, $inid, 4);
				# Updating datasets in the data table
				&UpdateDataTable($inid_innerdataid,$inid_outerdatait,$outid_innerdataid,$outid_outerdatait, 4);
				
				$DATARESCOUNT++;
			}
		
			print RES &InsertResult( $res_item );
			$RESCOUNT++;
			
		} else {
			die(print "\$res_item => " . Dumper($res_item) ."\n");
		} 
		
	}
	
	print " [$check_id_count]\n";

	print "\t=> Marking out results which were not used:\n\t";
	$check_id_count = 0;
	foreach my $out_key (@RFIDDIROUTDATASK) {
		$check_id_count++;
		(($check_id_count % 100) == 0) ? print ". [$check_id_count]\n\t" : print ".";
		&UpdateDirTable($out_key, $out_key, 1);
	}
	print " [$check_id_count]\n";
	
	##
	# update the rfids table to indicate that this rfid has been searched for in/out pairs
	$DBH->do(qq{UPDATE `$TABLE_RFIDS` set i='3', res=NOW() WHERE id = '$rfid'})
		or die("Could not update $TABLE_RFIDS -> $rfid: " . $DBH->errstr);
	
	
	##
	# uncomment next line to test only one rfid
	#exit;

}	

########################################################################
# ENDING

$DBH->disconnect();

my $summary = qq{
================================================
rfids:\t\t\t\t$RFIDCOUNT
Datasets:\t\t\t$DATACOUNT
Results (direction/data):\t$RESCOUNT ($DIRRESCOUNT/$DATARESCOUNT)
No Results (overlaps):\t\t$NORESCOUNT ($OVERLAPS)
================================================
};

printf RES $summary;
close(RES);

print $summary;

print"\n================================================\n";
print"SEARCHRES.PL COMPLETE";
print"\n================================================\n";

my @args = ( "/usr/bin/perl -I$SCRIPT_PATH " . $SCRIPT_PATH."meetings.pl");
system(@args) == 0 	
	or die "system @args failed: $?";
exit;

	
########################################################################
# SUBS
########################################################################
##
# Check for matching out data (from the direction out data and the data out data)
sub SearchMatchOut {
	
	my ($rfid, $toCheckItem) = @_;
	
	##
	# setting up return values
	my $result= { 'in' => $toCheckItem };
	
	##
	# get matching direction and data out datasets 
	my $dirMatch = &SearchMatchDir($toCheckItem);		# this gives the results which are like (1-3) -> (3-1) ( in pair/out pair pairs)
	my $dataMatch = &SearchMatchData($toCheckItem);		# this gives the results which are like (1-3) -> 1) ( in pair / out )
	
	## 
	# If we have no keys in $dirMatch and $dataMatch we have no matching out data.	
	return $result if (scalar(keys %$dirMatch) == 0) && (scalar(keys %$dataMatch) == 0);
		
	##
	# Else we have either a direction or a data result.
	#
	# Time min returns 1 if the dirMatch is the one closer to the IN time
	# and 0 if the dataMatch is the one which is closer to the IN time.
	# This works as well if one of the values is undefined.
	if( &TimeMin($dirMatch, $dataMatch) ) {
		return &DirMatchResult($result, $dirMatch); 
	} else {
		return &DataMatchResult($result, $dataMatch); 
	}
		 
}

##
# Put together a direction match result (1-3) -> (3-1)
sub DirMatchResult {
	my ($result, $dirMatch) = @_;
	
	$result->{'out'} = $dirMatch->{ shift(@{[keys %$dirMatch]}) };
	$result->{'typ'} = 3; # If the out match is a dataset from the direction table, set the typ to 3
	
	return $result;	
}

##
# Put together a data match result (1-3) -> (3-1)
sub DataMatchResult {
	my ($result, $dataMatch) = @_;
	
	$result->{'out'} = $dataMatch->{ shift(@{[keys %$dataMatch]}) };
	$result->{'typ'} = 4; # If the out match is a dataset from the data table, set the typ to 4
	
	return $result;
}


##
# search for matching direction out result (box).
sub SearchMatchDir {

	my $toCheckItem = shift(@_);
	my $dirMatch = ();
	
	##
	# Search for a matching direction out result item in TIME.
	for (my $i = 0; $i < scalar(@RFIDDIROUTDATASK); $i++) {
     
		# jump to next if the time is before the time we check for.
		next if $RFIDDIROUTDATA->{$RFIDDIROUTDATASK[$i]}->{'unix_time'} < $toCheckItem->{'unix_time'};
				
		##
		# if the time is after the time time for the data we check for, and the box
		# is the same, we've found a direction result (matching OUT data).
		if ($RFIDDIROUTDATA->{$RFIDDIROUTDATASK[$i]}->{'box'} eq $toCheckItem->{'box'}) {
			$dirMatch->{$RFIDDIROUTDATASK[$i]} = $RFIDDIROUTDATA->{$RFIDDIROUTDATASK[$i]}; # Found a match
			$dirMatch->{$RFIDDIROUTDATASK[$i]}->{'index'} = $i; # Store index for later deletion
			return $dirMatch;			
		}

	}
	
	return $dirMatch;
		
}

##
# search for matching direction out data (Antenna [box]1).
sub SearchMatchData {

	my $toCheckItem = shift(@_);
	my $dataMatch = ();
	
	##
	# Search for a matching data item in TIME.
	for (my $i = 0; $i < scalar(@RFIDOUTDATASK); $i++) {
		
		# jump to next if the time is before the time we check for.
		next if $RFIDOUTDATA->{$RFIDOUTDATASK[$i]}->{'unix_time'} < $toCheckItem->{'unix_time'};
		
		##
		# if the time is after the time time for the data we check for, and the box
		# is the same, we've found a DATA result (matching ANTENNA OUT data).
		if ($RFIDOUTDATA->{$RFIDOUTDATASK[$i]}->{'box'} eq $toCheckItem->{'box'}) {
			$dataMatch->{$RFIDOUTDATASK[$i]} = $RFIDOUTDATA->{$RFIDOUTDATASK[$i]}; # Found a match
			$dataMatch->{$RFIDOUTDATASK[$i]}->{'index'} = $i; # Store index for later deletion
			return $dataMatch;			
		}	
		
	}
	
	return $dataMatch;
}

##
# Insert a result entry into the database.  
#
# Update the direction / data table to mark the datasets as used in a result dataset.
#
sub InsertResult {
	
	my $res = shift;
	
	
	##
	# extracting rfid/table/kind of res data
	my $rfid		= $res->{'in'}->{'rfid'};	# get the rfid name	
	my $typ 		= $res->{'typ'};			# get the result typ
	my $nerv_index 	= $res->{'nerv_index'};		# index of nervousness
	
	############################################
	# extracting "in" data 
	my $inId 		= $res->{'in'}->{'id'};
	my $box 		= $res->{'in'}->{'box'};
	my $box_in		= $res->{'in'}->{'time'};
	my $inInnerId	= $res->{'in'}->{'innerdataid'};
	my $inOuterId	= $res->{'in'}->{'outerdataid'};	
	
	############################################
	# extracting "out" data
	
	my $outId = $res->{'out'}->{'id'};
	my $box_out = $res->{'out'}->{'time'};
	my $outInnerId = $res->{'out'}->{'innerdataid'};
	my $outOuterId	= $res->{'out'}->{'outerdataid'};
	
	##
	# calculate the time spent in the box in seconds
	my $dt = $res->{'out'}->{'unix_time'} - $res->{'in'}->{'unix_time'};
	 
	############################################
	# insert result into result table
	$INSRESSTH->execute($rfid, $box, $box_in, $box_out, $dt, $inId, $outId, $typ, $nerv_index)
		or die("Could not insert into $TABLE_RES: " . $DBH->errstr);
		
	##
	# get insert id and update the data.res_id and dir.res_id tables
	my $res_id = $DBH->{ q{mysql_insertid} };
	  
	##
	# update the data table
	$UPDATEDATARESIDS->execute($res_id, $inInnerId, $inOuterId, $outInnerId, $outOuterId)
	  or die( "Could not update $TABLE_DATA: " . $DBH->errstr );
	  
	##
	# update the data table
	$UPDATEDIRRESIDS->execute($res_id, $inId, $outId)
	  or die( "Could not update $TABLE_DIR: " . $DBH->errstr );
	  		
	##
	# return a result entry for the log File
	my $dtStr = sprintf "%02d:%02d:%02d",(gmtime $dt)[2,1,0];
	return sprintf($PRINTF,$rfid,$box,$box_in,$box_out,$dtStr);
}

##
# Update the i values of the data datasets. 
# 
# Set 'i' values on data table data
# 
sub UpdateDataTable {
	my ($inInnerId,$inOuterId,$outInnerId,$outOuterId,$i) = @_;
	
	die("UpdateDataTable => \$inInnerId is not defined") if !defined $inInnerId;
	die("UpdateDataTable => \$inOuterId is not defined ") if !defined $inOuterId;
	die("UpdateDataTable => \$outInnerId is not defined") if !defined $outInnerId;
	die("UpdateDataTable => \$outOuterId is not defined") if !defined $outOuterId;
	die("UpdateDataTable => \$i is not defined") if !defined $i;
	
	$UPDATEDATATABLE->execute($i,$inInnerId,$inOuterId,$outInnerId,$outOuterId)
		or die("Could not update $TABLE_DATA: " . $DBH->errstr);
}

##
# Update the i values of the direction datasets. 
# 
# Set 'i' values on direction table data
# hier irgendwo  
sub UpdateDirTable {
	my ($id_one, $id_two, $i) = @_;

	die("UpdateDirTable => \$id_one is not defined") if !defined $id_one;
	die("UpdateDirTable => \$id_two is not defined ") if !defined $id_two;
	die("UpdateDirTable => \$i is not defined") if !defined $i;
	
	$UPDATEDIRTABLE->execute($i, $id_one, $id_two)
		or die("Couldn't update $TABLE_DIR: " . $DBH->errstr );
}

##
# Get the minimal (unix-) time of the two results.
#
# If the direction match time is smaller or equal compared to the data match time
# return 0 else 1
sub TimeMin {

	my ($dirMatch, $dataMatch) = @_;
	
	return 1 if scalar(keys(%$dataMatch) == 0); # 1 if we have no data match 
	return 0 if scalar(keys(%$dirMatch) == 0); # 0 if we have no direction match
		
	my $dirTime = $dirMatch->{ shift(@{[keys %$dirMatch]}) }->{'unix_time'};
	my $dataTime= $dataMatch->{ shift(@{[keys %$dataMatch]}) }->{'unix_time'};

	return $dirTime <= $dataTime ? 1 : 0;	
}

##
# Delete the passed index from the passed array and return the new array.
sub DelItemFromArray {
	my ($index, $arr) = @_;
	
	splice(@$arr, $index, 1);
	
	return $arr;
}

##
# select statement return a hash ref with the results and the 'id' field as the key
sub SelArrayRef {
	
	my ($sth, $fields, $params) = @_;
	my $rfidData = {};
	
	$sth->execute(@$params)
		|| die("Could not execute statement: " . $DBH->errstr);;
	
	my %rec = ();
	$sth->bind_columns( map { \$rec{$_} } @$fields ) 
		|| die("Could not bind columns: " . $DBH->errstr);
	while ( $sth->fetchrow_arrayref ) {
		$rfidData->{ $rec{'id'} } = {%rec};
	}
	return ($rfidData);

}

##
# Search for overlapping results for a given result key
sub searchforOverlap {
	
	my ($res_sorted_key, $rfid_results) = @_;
	
	my @overlaps = ();
	my $i = 0;
	
	##
	# if the key's array consists only of one element we stop testing immediately
	# This happens when just one element ($res_sorted_key) is in the $res_sorted array.
	return \@overlaps if scalar(@RFIDRESSORTEDSK) == 0;
	
	##
	# Check if the entry time ('in') of the next result - sorted by time - is less then the exit time ('out')
	# of the result we check. If that's the case, these result we check is overlaping with the next 'in' result.    	
	while( $rfid_results->{$res_sorted_key}->{'out'}->{'unix_time'} >  $rfid_results->{ $RFIDRESSORTEDSK[$i] }->{'in'}->{'unix_time'} ) {
		
		push(@overlaps, $RFIDRESSORTEDSK[$i] );	
		
		##
		# if i is the same as the key's array length, get out the loop.
		$i++;
		last if $i == scalar(@RFIDRESSORTEDSK);
	} 

	return \@overlaps;
}

##
# Search for other antenna readings that were recorded while the result
sub searchOtherReadings {
	
	my ($res_value_sorted_key, $rfid_results) = @_;
	my $res_value = $rfid_results->{$res_value_sorted_key};
	my $dataset_overlaps = {};
		
	if($res_value->{'typ'} == 3) {
		
		$READINGS_DURING_DIRECTION_RESULT->execute( $res_value->{'out'}->{'innerdataid'}, $res_value->{'in'}->{'rfid'}, $res_value->{'in'}->{'time'}, $res_value->{'out'}->{'id'} )
			or die("Could not execute statement: " . $DBH->errstr);
			
		$dataset_overlaps = $READINGS_DURING_DIRECTION_RESULT->fetchall_hashref('id');
		
	} elsif($res_value->{'typ'} == 4) {
		
		$READINGS_DURING_DATA_RESULT->execute( $res_value->{'in'}->{'rfid'}, $res_value->{'in'}->{'time'}, $res_value->{'out'}->{'time'})
			or die("Could not execute statement: " . $DBH->errstr);
			
		$dataset_overlaps = $READINGS_DURING_DATA_RESULT->fetchall_hashref('id');
		
	}
		 
	return $dataset_overlaps;	
}

##
# check if the dataset overlaps hash contains only readings at 
# antenna 3 of the same box (nervous mouse looking out)
sub checkNervousness {
	
	my ($res_sorted_key, $rfid_results, $dataset_overlaps) = @_;

	my $res_value = $rfid_results->{$res_sorted_key};
	my $ant_to_check = $res_value->{'in'}->{'box'} . '3';
	
	for my $overlap_key ( keys %$dataset_overlaps) {
        
        if($dataset_overlaps->{$overlap_key}->{'ant'} != $ant_to_check) {
        	return 0;
        } 
    }
    
	
	return 1;
	
}


##
# Sort a hash ($data) by a value ($field), and return an array reference with the sorted keys.
sub sortedHashKeysArray {

	my ($data, $field) = @_;

	my @sortedKeys = ();	
	foreach my $key( sort{ $data->{$a}->{$field} <=> $data->{$b}->{$field} } keys(%$data) ) {
		push( @sortedKeys, $key); 
	}
	
	return \@sortedKeys;
	
}
