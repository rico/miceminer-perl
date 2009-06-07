#!/usr/bin/perl -w

##############################################################################
# Checks results for time overlaps.
#
# rleuthold@access.ch - 8.1.2009
##############################################################################

use strict;
use DBI;
use Date::Calc qw(Delta_YMDHMS);
use DateTime::Format::Strptime;

use lib 'lib';
use lib::DBHandler;
use lib::XMLPaths;
use lib::DBTables;

use Data::Dumper;

my $RFIDDATA;

##
# Global variables
my $DBH; 
my $TABLES		= DBTables->new();
my $PATHS		= XMLPaths->new();
my $PERLCONFIG	= PerlConfig->new();

# Paths / directories
my $DATA_PATH 	= $PATHS->get_path('data');

# database tables
my $TABLE_RFIDS	= $TABLES->get_table_name('rfids');
my $TABLE_RES	= $TABLES->get_table_name('results');
my $OVERLAPS	= 0;


####################################
# PREAMBLE
####################################


####################################
# open db connection
$DBH = DBHandler->new()->connect();

####################################
# MAIN
####################################
	
##
# get all results in hash with the rfid as key
my $RESULTS = $DBH->selectall_arrayref(qq{ SELECT id,rfid, box_in, box_out,inid, outid, i, UNIX_TIMESTAMP(box_in) as ts_box_in, UNIX_TIMESTAMP(box_out) as ts_box_out FROM $TABLE_RES}, { Slice => {} }) ||die("Cant't get results: " . $DBH->errstr);
#print "description => \$RESULTS: " . Dumper($RESULTS) ."\n";

##
# Build hash with rfids as keys and results as values
my $RFID_RES = {};
for my $res ( @$RESULTS) {
	$RFID_RES->{ $res->{'rfid'} }->{ $res->{'id'} } = $res;}

##
# Main loop
while ( my ($rfid, $values) = each(%$RFID_RES) ) {
	print "Checking $rfid \n";
	
	my @in_keys_sorted;
	
	##
	# sort values by ts_box_in and save the order in a helper array. 
	foreach my $res_id ( sort { $values->{$a}->{'ts_box_in'} <=> $values->{$b}->{'ts_box_in'} } keys %$values ) {
        #print "\t$res_id => " . $value->{$res_id}->{'box_in'} . "\n";
        &check_rfid($values->{$res_id});
        push(@in_keys_sorted, $values->{$res_id}->{'id'});
    }
    
	##
	# now check for overlaps
	my $position = 0;
	while ($position < ((scalar @in_keys_sorted) - 1) ) {
		&check_overlap($values, \@in_keys_sorted, $position );
		$position++;
	}
	
}

print "\nResults: " . scalar @$RESULTS ." Overlaps: $OVERLAPS\n";

####################################
# FINISH
####################################

$DBH->disconnect();

####################################
# SUBS
####################################

 
##
# Check for overlaps
sub check_overlap {
	
	my ($values, $in_keys_sorted, $position) = @_;
	
	my $start_position = $position;
	my $out_values_to_check = $values->{ @$in_keys_sorted[$position] };
	my $out_time_to_check = $out_values_to_check->{ 'ts_box_out' };
   	
   	$position++;
	
	while ( $position < ( scalar(@$in_keys_sorted)) ) {
		
		my $next_values = $values->{ @$in_keys_sorted[$position] }; 
		my $next_in_time = $next_values->{'ts_box_in'};
    	
    	if($next_in_time < $out_time_to_check) {
    		print "OVERLAP: " . $next_values->{'id'} ." / " . $out_values_to_check->{'id'} . "\n";
    		$OVERLAPS++;
    	} 		
 		$position++;	}
}

##
# Check for overlaps
sub check_rfid {
	
	my $res = shift(@_);
	print "\$res => " . Dumper($res) ."\n";
	exit;
	
}
