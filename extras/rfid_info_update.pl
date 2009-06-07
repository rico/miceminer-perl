#!/usr/bin/perl

##############################################################################
# This scripts imports a tab delimited file with the information for the rfids
# rfid \t sex \t weight \t implant data
#
# rfid: 10digits without (-) or spaces
# sex:  f/F/m/M
# weight: decimal number
# implant date: dd.mm.yy
#
#
# rleuthold@access.ch // 31.3.2008
##############################################################################
use strict;
use warnings;
use lib 'lib';

use Data::Dumper;
use DBHandler;

my $DBOBJ =  new DBHandler();
my $TABLE_RFIDS = $DBOBJ->get_table_name('rfids'); 
my $BASE_PATH 	= $DBOBJ->get_path('base_path');

##############################################################################
# SET THIS
##############################################################################
#my $INFILE 		= $BASE_PATH . "resources/Transponder2006_2008.txt";
my $INFILE 		= $BASE_PATH . "resources/transponder_info/transponder_summary_2008_07_27.txt";


##############################################################################
# PREAMBLE
##############################################################################

####################
# Db connection

my $DBH = $DBOBJ->connect();


##############################################################################
# MAIN
##############################################################################

##
# Reading File contents into a hash
#################################
# open LOGFILE for read
#print "reading file: $logFile\n";
open(RFID_INFO, "< $INFILE") or die("Can't open file $INFILE: $!");

##
# when the file is exported from excel it has ^M as the newline character ... replace them with a normal newline
open(RFID_CLEAN, "+> $INFILE.tmp") or die("Can't open file $INFILE.tmp: $!");

while (my $line = <RFID_INFO>) {
	
	if($line =~ m/\x0D/) {
		$line =~ s/^\s+//g;	
		chomp($line);
		$line =~ s/\x0D/\n/g;	
		print RFID_CLEAN $line;
	} else {
		$line =~ s/^\s+//g;	
		print RFID_CLEAN $line;
	}
}

close(RFID_CLEAN);
close(RFID_INFO);


rename("$INFILE.tmp", "$INFILE");
open(RFID_INFO, "< $INFILE") or die("Can't open file $INFILE: $!");

#################################

my %RFID_INFO = ();


my $counter = 0;
while (my $line = <RFID_INFO>) {
	
	next if($line =~ m/^\D{2}/);		# get rid of the nonsense lines
	#print $line;
	$_ = $line;

	#print $line ."\n";
	
	## 
	# extract the needed data
	my ($rfid, $sex, $weight, $implant_date) = ('','','','');
	my ($day_imp, $month_imp, $year_imp) = ('00','00','0000');	
	$_ = $line;
	($rfid) = /^(\w{10})/;
	
	if( !defined($rfid) ) {
		print "[ERROR] Couldn't extract rfid. Rfid must be of 10 digits length:\n\t-> $line\n";
		next;
	}
	
	$_ = $line;
	($sex) = /^\w{10}\t(f|m)/i;
	$_ = $line;
	($weight) = /^\w{10}\t\w\t(\d{1,2}\.?\d?)/i;
	$_ = $line;
	#($implant_date) = /(\d{2}\.\d{2}\.\d{2})$/;
	($day_imp, $month_imp, $year_imp) = /(\d{2})\.(\d{2})\.(\d{2})$/;

	
	# reformat rfid
	next if ($rfid eq '');
	$rfid =~ s/\-//g;
	
	# reformat sex
	$sex = '' if not defined $sex;
	$sex = lc($sex);
	
	# reformat weight
	$weight = 0 if not defined $weight;
	
	# reformat implant_date
	#print "Splitted: " . $day_imp ."-". $month_imp."-". $year_imp ."\n";
	my $implant_date_formatted = '20'.$year_imp.'-'.$month_imp.'-'.$day_imp;
	#print $implant_date_formatted . "\n";
	
	#print"$rfid\t$sex\t$weight\t$implant_date\n";
	$RFID_INFO{$rfid} = {'sex' => $sex,
						 'weight' => $weight,
						 'implant_date' => $implant_date_formatted
						};
	$counter++;
	#exit;

	
}

#print " => \$RFID_INFO: " . Dumper(\%RFID_INFO) ."\n";
#exit;
#print"Counter: $counter\n";
close(RFID_INFO);

##
# Updating rfid db table
####################################
my $UPDATE_RFID_STH = $DBH->prepare(qq{INSERT INTO $TABLE_RFIDS (id, sex, weight, implant_date) VALUES(?,?,?,?) ON DUPLICATE KEY 
											UPDATE sex=?, weight=?, implant_date=?}) 
									or die("Could not prepare statement to update rfid table: " . $DBH->errstr);	


foreach my $rfid (keys %RFID_INFO) {
	
	print"Updating/Creating rfid: $rfid\n";
	my  $values = $RFID_INFO{$rfid};
	$UPDATE_RFID_STH->execute($rfid, $values->{'sex'}, $values->{'weight'}, $values->{'implant_date'}, $values->{'sex'}, $values->{'weight'}, $values->{'implant_date'} )
					or die("Could not execute insert: " . $DBH->errstr);
					
	if( $UPDATE_RFID_STH->rows == 0 ) {
		print "[WARNING] Could not insert/update data for rfid: $rfid\n";
	}

}


$DBH->disconnect();
print"\n-------------------\n";
print"END";
print"\n-------------------\n";

