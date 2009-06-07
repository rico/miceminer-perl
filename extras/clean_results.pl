#!/usr/bin/perl -w

##############################################################################
# Checks all results for other antenna readings for that mouse that took place 
# while the mouse was in the box.
#
# rleuthold@access.ch - 17.4.2009
##############################################################################

use strict;
use DBI;
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

# database tables
my $TABLE_DATA	= $TABLES->get_table_name('data');
my $TABLE_DIR	= $TABLES->get_table_name('direction_results');
my $TABLE_RES	= $TABLES->get_table_name('results');
my $UNCLEAN	= 0;
my $UNCLEAN_TYPE_4	= 0;
my $UNCLEAN_TYPE_3	= 0;
my $CLEAN	= 0;
my $NERVOUS = 0;
my $RFIDDATA;

####################################
# PREAMBLE
####################################
print q{
##############################################################################
# Checks all results for other antenna readings for that mouse that took place 
# while the mouse was in the box.
#
# rleuthold@access.ch - 17.4.2009
##############################################################################
};


####################################
# open db connection
$DBH = DBHandler->new()->connect();

####################################
# MAIN
####################################

##
# Prepare statements
my $READINGS_DURING_DIRECTION_RESULT = $DBH->prepare(qq{SELECT * FROM $TABLE_DATA WHERE rfid=? AND time > ? AND time < 
	(SELECT time FROM $TABLE_DATA WHERE id = (SELECT innerdataid  FROM $TABLE_DIR WHERE id = ?));
})	
	or die("Could not prepare statement to check for other readings in table $TABLE_DATA: " . $DBH->errstr);

my $READINGS_DURING_DATA_RESULT = $DBH->prepare(qq{SELECT * FROM $TABLE_DATA WHERE rfid=? AND time > ? AND time < ?})	
	or die("Could not prepare statement to check for other readings in table $TABLE_DATA: " . $DBH->errstr);	

	
##
# get all results in hash with the rfid as key
print "\nGettig results ...\n";
my $RESULTS = $DBH->selectall_arrayref(qq{ SELECT id,rfid, box, box_in, box_out,inid, outid, i, dt FROM $TABLE_RES}, { Slice => {} }) ||die("Cant't get results: " . $DBH->errstr);
#print "description => \$RESULTS: " . Dumper($RESULTS) ."\n";

##
# Build hash with rfids as keys and results as values
print "Rearrangin results ...\n";
my $RFID_RES = {};
for my $res ( @$RESULTS) {
	$RFID_RES->{ $res->{'rfid'} }->{ $res->{'id'} } = $res;
}

##
# Main loop
while ( my ($rfid, $values) = each(%$RFID_RES) ) {
	print "\n[Checking $rfid]\n";
	
	##
	# sort values by ts_box_in and save the order in a helper array. 
	foreach my $res_id (keys %$values ) {
		my $res_value = $values->{$res_id};
        my $other_readings = &check_clean($res_value);
        
        if(keys %$other_readings) {
        	
        	##
        	# check if the other_readings hash contains only readings at 
        	# antenna 3 of the same box (nervous mouse)
        	if( &check_same_box($res_value, $other_readings) ) {
        		print "nervous => $res_id (" . scalar(keys(%$other_readings)) . ")\n";
        		$NERVOUS++;
        		$CLEAN++;
        		#print "\$res_value => " . Dumper($res_value) ."\n";
        		#print "\$other_readings => " . Dumper($other_readings) ."\n";
        	} else {
        		
        		my $dt = $res_value->{'dt'};
	        	print "\tnot clean => $res_id ";

	        	if( $res_value->{'i'} == 3) {
	        		$UNCLEAN_TYPE_3++;
	        		#print "[typ: 3;dt: $dt]";
	        	} else {
	        		$UNCLEAN_TYPE_4++;
	        		#print "[typ: 4;dt: $dt]";
	        	}
	        	
	        	#print "\n\t(" . join(",", keys %$other_readings ) . ")\n";
	        	$UNCLEAN++;        		
        	}

         } else{
         	$CLEAN++;
         }
    }
}

####################################
# FINISH
####################################

my $percent_clean = ($CLEAN * 100 )/ (scalar @$RESULTS);
my $percent_unclean = ($UNCLEAN * 100 )/ (scalar @$RESULTS); 

print"\n=========================================================================================\n";
print "\nResults:\t\t" . scalar @$RESULTS . "\n";
print "Clean [%] (nervous):\t\t\t $CLEAN [". $percent_clean . "] ($NERVOUS)\n";
print "Not Clean [%] (typ 3/ typ 4):\t $UNCLEAN [" . $percent_unclean . "] ($UNCLEAN_TYPE_3/$UNCLEAN_TYPE_4)\n";
print"\n=========================================================================================\n";

$DBH->disconnect();

####################################
# SUBS
####################################

##
# Check for other readings for that rfid during the result time 
sub check_clean {
	
	my $res = shift(@_);
	my $other_readings = {};
		
	if($res->{'i'} == 3) {
		
		$READINGS_DURING_DIRECTION_RESULT->execute( $res->{'rfid'}, $res->{'box_in'}, $res->{'outid'})
			or die("Could not execute statement: " . $DBH->errstr);
			
		$other_readings = $READINGS_DURING_DIRECTION_RESULT->fetchall_hashref('id');
		
	} elsif($res->{'i'} == 4) {
		
		$READINGS_DURING_DATA_RESULT->execute( $res->{'rfid'}, $res->{'box_in'}, $res->{'box_out'})
			or die("Could not execute statement: " . $DBH->errstr);
			
		$other_readings = $READINGS_DURING_DATA_RESULT->fetchall_hashref('id');
		
	}
		 
	return $other_readings;	
}

##
# check if the other_readings hash contains only readings at 
# antenna 3 of the same box (nervous mouse)
sub check_same_box {
	
	my ($res_value, $overlaps) = @_;
	
	my $ant_to_check = $res_value->{'box'} . '3';
	
	for my $overlap_key ( keys %$overlaps ) {
        
        if($overlaps->{$overlap_key}->{'ant'} != $ant_to_check) {
        	return 0;
        } 
    }
	
	return 1;
	
}
