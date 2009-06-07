#!/usr/bin/perl -w

##############################################################################
# Check if we have "valid" counter results
#
# rleuthold@access.ch // 25.9.2007
##############################################################################


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
my $DAYS_IN_DB = $DBH->selectcol_arrayref("SELECT DISTINCT DATE(time) FROM $TABLE_DATA ORDER BY time")
		or die("Could not execute statement to get days in database: " . $DBH->errstr);	
		
my $DAYS_RFID_COUNTED = $DBH->selectcol_arrayref("SELECT DISTINCT day FROM $TABLE_BOX_COUNT ORDER BY day")
		or die("Could not execute statement to get days in $TABLE_RFIDS_COUNT: " . $DBH->errstr);	
		
#print " => \$DAYS_IN_DB: " . Dumper($DAYS_IN_DB) ."\n";
#print " => \$DAYS_RFID_COUNTED: " . Dumper($DAYS_RFID_COUNTED) ."\n";

##
# Finding differences on data days and count days
my @DIFF_DAYS;
my %days = ();

foreach my $day (@$DAYS_IN_DB, @$DAYS_RFID_COUNTED) {
	$days{$day}++;
}

foreach $day (keys %days) {

	if($days{$day} == 1) {
		push(@DIFF_DAYS, $day);
	}

}

print "------------------------------------\n";		
print "Found differences in Days:\n\n\t" . join("\n\t",@DIFF_DAYS) . "\n";
print "------------------------------------\n";



print "------------------------------------\n";		
print "Checking counts for the following dates:\n\n\t" . join("\n\t",@$DAYS_IN_DB) . "\n";
print "------------------------------------\n";
		
foreach my $DAY (@$DAYS_IN_DB) {

	print "------------------------------------\n";		
	print "Checking day: " . $DAY . "\n";
	print "------------------------------------\n\n";		
	
	##
	# Checking boxes count
	my $box_sets_day = shift(@{[$DBH->selectrow_array("SELECT COUNT(id) FROM $TABLE_BOX_COUNT WHERE day ='$DAY'")]});
	
	if($box_sets_day == 40) {
		print"box sets: $box_sets_day\tOK\n";	
	} else {
		print"box sets: $box_sets_day\tNOT OK!!!\n";	
	}

	##
	# Checking antenna count
	my $ant_sets_day = shift(@{[$DBH->selectrow_array("SELECT COUNT(id) FROM $TABLE_ANT_COUNT WHERE day ='$DAY'")]});
	
	if($ant_sets_day == 80) {
		print"ant sets: $ant_sets_day\tOK\n";
	} else {
		print"ant sets: $ant_sets_day\tNOT OK!!!\n";
	}
	
	##
	# Checking rfid count
	my $rfids_sets_day = shift(@{[$DBH->selectrow_array("SELECT COUNT(id) FROM $TABLE_RFIDS_COUNT WHERE day ='$DAY'")]});
	
	print"rfids sets: $rfids_sets_day\n";
	
	
	
	##
	# uncomment to test one day
	#exit;
}

print "\n--------------------------\n";
print "Checks complete\n";
print "--------------------------\n";

$DBH->disconnect();