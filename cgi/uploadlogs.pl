#!/usr/bin/perl -w

##########################################
# very simple script to upload files,
# check for existing logfiles in the
# database and for chronological import
# ---------------------------------------
# rleuthold@access.ch // 30.01.2009s
#########################################

use strict;
use CGI;
use DBI;
use Fcntl;

use lib 'lib';
use lib::DBTables;
use lib::DBHandler;
use lib::XMLPaths;
use Data::Dumper;

####################################################################################################
# Change this to the format you need
my $REGEX = '(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.txt';	# The regex for the file format
#
####################################################################################################

##
# variable declarations
my $DBH; 
my $TABLES		= DBTables->new();
my $PATHS		= XMLPaths->new();

my $DEBUG 		= 0;
my $TABLE_LOGS	= $TABLES->get_table_name('logfiles');	# logfiles table
my $UPLOADDIR 	= $PATHS->get_path('data');				# upload directory

##
# CGI object
my $CGI = new CGI();

##
# header
print $CGI->header('text/xml');
##
# getting parameters
my  $PARAMS = $CGI->Vars;
my $FILE;

if( !defined($CGI->param('Filedata')) ) {
	print"<info><status>0</status><text>filename missing</text></info>";
	exit;
} else {
	$FILE = $CGI->param('Filedata');
	$FILE =~ s/.*[\/\\](.*)/$1/;
}

##
# print some debug stuff
&Debug if ($DEBUG == 1);


##
# check if the specified upload directory has the right permission
if (!(-w $UPLOADDIR)) {
	print"<info><status>0</status><text>Upload directory '$UPLOADDIR' is not writable</text></info>";
	exit;
}

#############################################################################
# open db connection
$DBH = DBHandler->new()->connect();
#############################################################################

##
# check if filename has right form
if ( $FILE !~ /$REGEX/ ) {
	print "<info><status>0</status><file>$FILE</file><text>The filename '$FILE' doesn't have the required format (yyyymmdd_hhmmss.txt).</text></info>";
	exit;
}

##
# check if the file is already imported or if its older than the latest one imported
$_ = $FILE;
my ($y, $m, $d, $h, $min, $sec) = /$REGEX/;

if (&CheckImport($y."-".$m.$d.$h.$min."00") == 1) {
	print "<info><status>0</status><file>$FILE</file><text>The file '$FILE' is already in the database! Reimport is not possible.</text></info>";
	exit;
}

if (&CheckLatest($y.$m.$d.' '.$h.$min."00") == 1) {
	print "<info><status>0</status><file>$FILE</file><text>The file '$FILE' seems to be older then the latest one imported into the database. Due to technical reason, you can not upload this file.</text></info>";
	exit;
}

if (&CheckFuture($y."-".$m.$d.$h.$min."00") == 1) {
	print "<info><status>0</status><file>$FILE</file><text>The file '$FILE' seems to come from the future. Don't fool me!</text></info>";
	exit;
}

##
# check if the file is already in the upload directory
if (-e $UPLOADDIR.$FILE) {
	print "<info><status>1</status><file>$FILE</file><text>The file '$FILE' has already been uploaded to the server and will be imported.</text></info>";
} else {
	open UPLOAD, ">$UPLOADDIR/$FILE" or die ("<error>could not open upload file $FILE</error>");  
	binmode UPLOAD; 
	
	while ( <$FILE> ) { 
		print UPLOAD; 
	} 
	
	close UPLOAD; 
	
	print "<info><status>1</status><file>$FILE</file><text>The file '$FILE' has successfully been uploaded to the server and will be imported and analyzed.</text></info>";
}


#########################################################
# SUBS
#########################################################

sub Debug{
	
	sysopen(TEST, "/tmp/uploadlogs_log.txt", O_RDWR|O_CREAT, 0777) or die $!;

	printf TEST "--------------------------------\n";
	foreach my $param (keys %$PARAMS) {
		printf TEST $param . "->" . $PARAMS->{$param} . "\n";
	}
	printf TEST "Referer: " . $CGI->user_agent();
	printf TEST "\n--------------------------------\n";
	
	close(TEST);

}

##
# checks if the file is already imported
sub CheckImport {

	my $short = shift;
	
	my $checkSQL = qq{SELECT count(logfile) FROM $TABLE_LOGS WHERE logfile='$FILE'};
	
	if (($DBH->selectrow_array($checkSQL,undef)) == 1) {
		return 1;
	} else {
		return 0;
	}

	
	return ();

}

##
# checks if the file we are about to import is the younger than the ones already imported
sub CheckLatest {

	my $short = shift;

	my ($date, $time) = split('\s+', $short);
	#print"date: $date\ntime: $time\n";
	my $checkSQL = qq{SELECT MAX(end) FROM $TABLE_LOGS};

	my ($maxDate, $maxTime) = split('\s+',$DBH->selectrow_array($checkSQL,undef));
	$maxDate =~ s/-//g;
	$maxTime =~ s/://g;
	
	if ($date < $maxDate) { 
		if ($time < $maxTime) {
			return 1;
		}
	}
	
	return 0;

}


##
# checks if the file we are about to import is in the future
sub CheckFuture {

	my $short = shift;

	my ($logYear, $logDatetime) = split('-', $short);
	#print"logYear: $logYear\n";
	#print"logDatetime: $logDatetime\n";
	
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
	
	my $actYear = sprintf("20%02d", $year % 100);
	#print"actYear: $actYear\n";
	my $actDatetime = sprintf("%02d%02d%02d%02d%02d",($mon+1),$mday,$hour,$min,$sec);
	#print"actDatetime: $actDatetime\n";
	
		
	if ($actYear <= $logYear && $actDatetime < $logDatetime) {
		return 1;
	} 
	
	return 0;

}

