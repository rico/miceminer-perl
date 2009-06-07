#!/usr/bin/perl

##############################################################################
# This scripts searches for meetings between two mice which is 
# the time two mice spend in the boxes together.
#
# rleuthold@access.ch - 9.1.2009
##############################################################################
use strict;
use warnings;

use Data::Dumper;
use Date::Calc qw(:all);

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

# database tables
my $TABLE_RFIDS	= $TABLES->get_table_name('rfids');
my $TABLE_RES	= $TABLES->get_table_name('results');
my $TABLE_BOX 	= $TABLES->get_table_name('boxes');
my $TABLE_MEETINGS = $TABLES->get_table_name('meetings');

my $SCRIPT_PATH	= $PERLCONFIG->get_scriptsfolder();

my $RFIDS;
my $BOXES;
my $BOX;

#############################################################################
# open db connection
$DBH = DBHandler->new()->connect();
#############################################################################

print"\n================================================\n";
print"STARTING MEETINGS.PL";
print"\n================================================\n";

##############################################################################
# MAIN
##############################################################################
##
# Preparing sql statements
##
# rfid res data for given box, exclude under three seconds stay results and the ones which are already searched for meetings
my $RFIDS_FOR_BOX 	= $DBH->prepare(qq{SELECT id,rfid, UNIX_TIMESTAMP(box_in), UNIX_TIMESTAMP(box_out), dt FROM `$TABLE_RES`
										WHERE box= ? AND dt > '00:00:03' AND meetings=1})
		or die("Could not prepare statement to get rfid dir results: " . $DBH->errstr);	

##
# add meetings to the db
my $MEETINGS_TO_DB 	= $DBH->prepare(qq{INSERT INTO `$TABLE_MEETINGS`(`box`,`rfid_from`, `res_id_from`, `rfid_to`, `res_id_to`, `from`, `to`, `dt`, `typ`) 
											VALUES(
												?,
												(SELECT rfid FROM `$TABLE_RES` WHERE id= ?),?,(SELECT rfid FROM `$TABLE_RES` WHERE id= ?),?,
												(SELECT FROM_UNIXTIME(?)),(SELECT FROM_UNIXTIME(?)),
												(SELECT TIMEDIFF(FROM_UNIXTIME(?), FROM_UNIXTIME(?))),
												?
											)
										})
		or die("Could not prepare statement to insert meetings data: " . $DBH->errstr);	


##
# get all rfid and boxes data
$RFIDS = $DBH->selectall_hashref("SELECT id FROM rfid", 'id');
$BOXES = $DBH->selectcol_arrayref("SELECT id FROM $TABLE_BOX");

#print " => \$RFIDS: " . Dumper($RFIDS) ."\n";
#print " => \$BOXES: " . Dumper($BOXES) ."\n";
##
# Loop through boxes
my @RESULT_IDS_SEARCHED = ();
foreach $BOX (@$BOXES) {

		##
		# get results for this box
		$RFIDS_FOR_BOX->execute($BOX)
			or die("Could not execute select: " . $DBH->errstr);

		##
		# the hash looks like this rfid=>{results for rfid and box}
		my $rfids_results = {};
		#my %rfid_res_count = ();
		
		my $box_res = sel_array_ref($RFIDS_FOR_BOX, 'id', [qw{id rfid box_in box_out dt}]); # get tableData  for the current rfid
		#print " => \$box_res: " . Dumper($box_res) ."\n";
		
		while ( my ($key, $value) = each(%$box_res) ) {
			
			my $rfid = $value->{'rfid'};
			#$rfid_res_count{$rfid}++;
			
			##
			# check if the rfid is already a key with an anonymous hash as value
			# if true add the new key/value pair to the hash, if not create the 
			# anonymoous hash and add the first key/value pair
			if (defined $rfids_results->{$rfid}) {
				$rfids_results->{$rfid}->{$key} = $value;
			} else {
				$rfids_results->{$rfid} = { $key => $value };
			}

	    }
	    
	    #print " => \$rfid_res: " . Dumper($rfid_res) ."\n";
	    #print " => \$rfid_res_count: " . Dumper(\%rfid_res_count) ."\n";
	   print"------ BOX $BOX ------------------------------------------\n";
	    
	    ##
	    # Loop through each rfid and get the assiciated results
	    # meaning that this results have a time overlap (box_in, box_out)
	    
	    my $meetings = {};
	    my $rfid_count = 0;

	    for my $rfid ( keys %$rfids_results ) {
	    
	    	($rfid_count % 10 == 0) ? print "\n[$rfid] " : print "[$rfid] ";
	    	$rfid_count++;
	    	
			##
	    	# Loop through all results for this rifd and search for overlapping time
	    	# results
	    	my $rfid_results = $rfids_results->{$rfid};
	    	for my $result_id ( keys %$rfid_results ) {
	    		my $assoc_result_id = &search_meetings($rfid,$result_id, $rfids_results);
	    		push(@RESULT_IDS_SEARCHED, $result_id);
	    		
				$meetings->{$rfid}->{$result_id} = $assoc_result_id if(keys %$assoc_result_id);

			}
			
			##
			# delete the rfid with results from the hash
			# to avoid duplicate searches
			delete $rfids_results->{$rfid};
			
			##
			# uncomment next line to test only one rfid for this box
			#last;
		}
		
		
		##
		# add meetings for this box to the db the db
		if(keys %$meetings) {
			
			&meetings_to_db($BOX, $meetings)
				
		}
		
		print"\n------------------------------------------------------\n";
		print" $rfid_count rfids searched for box $BOX";
		print"\n------------------------------------------------------\n\n";
		
		##
		# uncomment next line to test only one box
		#last;
		
}

##
# updating res table to indicate which results have been searched for meetings

my $SET_MEETING_TRUE = $DBH->prepare(qq{UPDATE $TABLE_RES SET meetings='true' WHERE id= ?}) or die("Could not prepare statement to update meetings in $TABLE_RES: " . $DBH->errstr);	

print"setting searched result sets as searched for meetings ... ";
foreach my $result_id (@RESULT_IDS_SEARCHED) {
	$SET_MEETING_TRUE->execute($result_id) or die("Could not execute statement to update meetings in $TABLE_RES: " . $DBH->errstr);	
}

print"[OK]\n";

print"\n================================================\n";
print"MEETINGS.PL COMPLETE";
print"\n================================================\n";

$DBH->disconnect();

my @args = ( "/usr/bin/perl -I$SCRIPT_PATH " . $SCRIPT_PATH."counter.pl");
system(@args) == 0 	
	or die "system @args failed: $?";
exit;

##############################################################################
# SUBS
##############################################################################
##
#  search with whom this rfid spent time with
sub search_meetings {

	my ($rfid, $result_id, $rfids_results) = @_;
	
	my $meetings_for_rfid = {}; # hash for the assoc (stay together) results
	
	#print"rfid: $rfid\tresult_id: $result_id" ."\n";
	
	##
	# values to check against (passed rfid times)
	my $box_in_rfid		= $rfids_results->{$rfid}->{$result_id}->{'box_in'};
	my $box_out_rfid 	= $rfids_results->{$rfid}->{$result_id}->{'box_out'};
	
	my $result_data = $rfids_results->{$rfid}->{$result_id};
	
	#print "search_meetings => \$rfid_res_id: " . Dumper($rfid_res_id) ."\n";
	#print "search_meetings => \$box_in_rfid: " . Dumper($box_in_rfid) ."\n";
	#print "search_meetings => \$box_out_rfid: " . Dumper($box_out_rfid) ."\n";
	
	
	##
	# looping over rfids to check against (rfid_check)
	for my $rfid_check ( keys %$rfids_results) {
		
		next if ($rfid_check eq $rfid); # skip if the rfid is the same as the rfid we passed 
		
		##
		# looping over results for the rfid_check
		my $rfid_results = $rfids_results->{$rfid_check};
		
		for my $result_check_id ( keys %$rfid_results ) {
				
			
			##
			# values to check (rfid_check_result)
			my $box_in_rfid_check	= $rfid_results->{$result_check_id}->{'box_in'};
			my $box_out_rfid_check 	= $rfid_results->{$result_check_id}->{'box_out'}; 
			
			my $result_check_data = $rfid_results->{$result_check_id};
			
			#print "search_meetings => \$box_in_rfid_check: " . Dumper($box_in_rfid_check) ."\n";
			#print "search_meetings => \$box_out_rfid_check: " . Dumper($box_out_rfid_check) ."\n";

			
			##
			# check for cases we have always two datasets to check against each other ds1, ds2.
			# And we always have to check when one ds1,ds entered left (box_in, box_out)
			#
			# We can exclude the results matching the following conditions:
			# 
			# 1.)ds1 left before ds2 entered
			#	ds1.out < ds2.in
			next if ( $box_out_rfid < $box_in_rfid_check );
			
			# 
			# 2.) ds1 entered after ds2 left
			#	ds1.in > ds2.out 
			next if ( $box_in_rfid > $box_out_rfid_check );
			
			##
			# We have these cases to get a valid result or 'stayed together in the box'
			#
			# 1.) ds2 is in the range of ds1:
			#	ds1.in <= ds2.in AND ds1.out >= ds2.out
			#
			# In this case we catch the possibility that the ds1.in == ds2.in AND ds1.out == ds2.out (with the <= / >= operators) 
			if( $box_in_rfid <= $box_in_rfid_check && $box_out_rfid >= $box_out_rfid_check ) {
			
				my $assoc_res = &add_meeting_res($box_in_rfid_check, $box_out_rfid_check);
				#print"ds2 is in the range of ds1:\t" . Dumper($assoc_res) . "\n";
				#print "\$result_data: " . Dumper($result_data) ."\n";
				#print "\$result_check_data: " . Dumper($result_check_data) ."\n";
				#exit;
				$assoc_res->{'typ'} = 1;
				$meetings_for_rfid->{$result_check_id} = $assoc_res;
				next;
			}
			#	
			# 2.) ds1 is in the range of ds2: 
			#	ds1.in > ds2.in AND ds1.out < ds2.out
			elsif( $box_in_rfid > $box_in_rfid_check && $box_out_rfid < $box_out_rfid_check ) {

				my $assoc_res = &add_meeting_res($box_in_rfid, $box_out_rfid);
				#print"ds1 is in the range of ds2:\t" . Dumper($assoc_res) . "\n";
				#print "\$result_data: " . Dumper($result_data) ."\n";
				#print "\$result_check_data: " . Dumper($result_check_data) ."\n";				
				#exit;
				$assoc_res->{'typ'} = 2;
				$meetings_for_rfid->{$result_check_id} = $assoc_res;
				next;
			}
			#	
			# 3.) ds1 entered after ds2 and ds2 left while ds1 was still in the box:
			#	ds1.in > ds2.in AND ds1.in < ds2.out ds2.out AND ds1.out > ds2.out
			elsif( $box_in_rfid > $box_in_rfid_check && $box_in_rfid < $box_out_rfid_check && $box_out_rfid > $box_out_rfid_check ) {
			
				my $assoc_res = &add_meeting_res($box_in_rfid, $box_out_rfid_check);
				#print"ds1 entered after ds2 and ds2 left while ds1 was still in the box:\t" . Dumper($assoc_res) . "\n";
				#print "\$result_data: " . Dumper($result_data) ."\n";
				#print "\$result_check_data: " . Dumper($result_check_data) ."\n";				
				#exit;
				
				$assoc_res->{'typ'} = 3;
				$meetings_for_rfid->{$result_check_id} = $assoc_res;
				next;
			}
			#	
			# 4.) ds2 entered after ds1 and ds1 left while ds2 was still in the box:
			#	ds1.in < ds2.in AND ds1.out > ds2.in AND ds1.out < ds2.out
			elsif( $box_in_rfid < $box_out_rfid_check && $box_out_rfid > $box_in_rfid_check && $box_out_rfid < $box_out_rfid_check ) {
			
				my $assoc_res = &add_meeting_res($box_in_rfid_check, $box_out_rfid);
				#print"ds2 entered after ds1 and ds1 left while ds2 was still in the box:\t" . Dumper($assoc_res) . "\n";
				#print "\$result_data: " . Dumper($result_data) ."\n";
				#print "\$result_check_data: " . Dumper($result_check_data) ."\n";				
				#exit;
				$assoc_res->{'typ'} = 4;
				$meetings_for_rfid->{$result_check_id} = $assoc_res;
			}

		} # END looping over results
				
	} # END looping over rfids
	
	return $meetings_for_rfid;

}


##
# return an anonymouse hash consisting of the data needed for a 'meeting' result
sub add_meeting_res {

	my ($from, $to) = @_;
	
	my @dt = localtime($to - $from);
	my $dt_string = sprintf( "%02d:%02d:%02d",$dt[2] - 1, $dt[1], $dt[0]);
	
	return  {	'from'	=> $from,
				'to'	=> $to,
				'dt'	=> $dt_string

			};			

}

##
# add meetings for rfid to database
sub meetings_to_db {

	my ($box,$meetings) = @_;
	
	##
	# looping over rfids
	for my $rfid ( sort keys %$meetings ) {
	
		my $rfid_meetings = $meetings->{$rfid};
		
		##
		# looping over result ids
		for my $res_id ( sort keys %$rfid_meetings ) {
			
			my $res_id_meetings = $rfid_meetings->{$res_id};
			
			##
			# Looping over the result ids for the matching data
			for my $res_id_match ( sort keys %$res_id_meetings) {
			
				my $from = $res_id_meetings->{$res_id_match}->{'from'};
				my $to = $res_id_meetings->{$res_id_match}->{'to'};
				my $typ = $res_id_meetings->{$res_id_match}->{'typ'};				
				
				##
				# execute insert statement 
				$MEETINGS_TO_DB->execute($box, $res_id, $res_id, $res_id_match, $res_id_match, $from, $to, $to, $from, $typ )
					or die("Could not execute insert: " . $DBH->errstr);
				
			}
		}	
	}
	
	return undef;
}

##
# Returns a hashref for a dbi result with the desired field ($key) as key
sub sel_array_ref {

	my ($sth, , $key, $fields) = @_;
	my $result = {};	
    my %rec =();
    $sth->bind_columns(map {\$rec{$_}} @$fields);

	while ($sth->fetchrow_arrayref) { 
		#print "rec: " . Dumper(\%rec) ."\n";
		$result->{$rec{$key}} = {%rec};
	};
	
	
	return $result;

}