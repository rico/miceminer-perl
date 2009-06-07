#!/usr/bin/perl -w

##############################################################################
# This scripts imports a tab delimited file with the information for the boxes
# Line Format: boxid \t xcoord \t ycoord \t segment
#
# boxid: 2 characters
# xcoord:  integer positive
# ycoord: integer positive
# segment: A or B or C or D
#
# rleuthold@access.ch // 30.1.2009
##############################################################################
use strict;
use CGI;
use DBI;

use lib 'lib';
use lib::DBHandler;
use lib::XMLPaths;
use lib::DBTables;
use XML::Writer;
use Data::Dumper;
use File::Copy;

my $DBH; 
my $TABLES		= DBTables->new();
my $PATHS		= XMLPaths->new();

my $TABLE_ANTENNAS 	= $TABLES->get_table_name('antennas'); 	# table antennas
my $UPLOADDIR 		= $PATHS->get_path('data');				# upload directory


##
# CGI object
my $CGI = new CGI();

##
# header
print $CGI->header('text/xml');


##
# xml feedback
my $FEEDBACK = '';
my $XMLWRITER = new XML::Writer(OUTPUT => $FEEDBACK, NEWLINES => 1);
$XMLWRITER->startTag("feedback");

##
# getting parameters
my  $PARAMS = $CGI->Vars;
#my $FILE = "/Users/rico/Desktop/boxes_test.txt";	# file reference
my $FILE;	# file reference
# 

if( !defined($CGI->param('Filedata'))  ) {
	&Exit("filename missing");
} else {
	$FILE = $CGI->param('Filedata');
	$FILE =~ s/.*[\/\\](.*)/$1/;
}

##
# check if the specified upload directory has the right permission
if (!(-w $UPLOADDIR)) {
	&Exit("Upload directory '$UPLOADDIR' is not writable");
}


##############################################################################
# PREAMBLE
##############################################################################

#############################################################################
# open db connection
$DBH = DBHandler->new()->connect();
#############################################################################

#my $BOXES = $DBH->selectall_hashref("SELECT id FROM $TABLE_BOXES",'id');
# 
#print " => \$BOXES: " . Dumper(\$BOXES) ."\n";
#exit;

##############################################################################
# MAIN
##############################################################################

##
# Reading File contents into a hash
#################################

&RemoveFile();

open UPLOAD, ">$UPLOADDIR/$FILE" or &Exit("Could not open file '$UPLOADDIR/$FILE'");  
binmode UPLOAD; 

while ( <$FILE> ) { 
	print UPLOAD; 
} 

close UPLOAD; 

# open LOGFILE for read
#print "reading file: $logFile\n";
open(ANTENNA_INFO, "< $UPLOADDIR/$FILE") or &Exit("Could not open file '$FILE'");

##
# when the file is exported from excel it has ^M as the newline character ... replace them with a normal newline
##
open(ANTENNA_CLEAN, "+> $UPLOADDIR/$FILE.tmp") or &Exit("Can't open file '$UPLOADDIR/$FILE.tmp': $!");

while (my $line = <ANTENNA_INFO>) {
	
	if($line =~ m/\x0D/) {
		$line =~ s/^\s+//g;	
		chomp($line);
		$line =~ s/\x0D/\n/g;	
		print ANTENNA_CLEAN $line;
	} else {
		$line =~ s/^\s+//g;	
		print ANTENNA_CLEAN $line;
	}
}

close(ANTENNA_CLEAN);
close(ANTENNA_INFO);


rename("$UPLOADDIR/$FILE.tmp", "$UPLOADDIR/$FILE");
open(ANTENNA_INFO, "< $UPLOADDIR/$FILE") or &Exit("error","Can't open file '$UPLOADDIR/$FILE': $!");


##
# Preparing the statements
####################################
my $UPDATE_ANT_STH = $DBH->prepare(qq{INSERT INTO $TABLE_ANTENNAS (id) VALUES(?) ON DUPLICATE KEY 
											UPDATE id=?}) 
									or &Exit("Could not prepare statement to update $TABLE_ANTENNAS table: " . $DBH->errstr);

##
# getting boxes in the db
my $ANTENNAS = $DBH->selectall_hashref("SELECT id FROM $TABLE_ANTENNAS",'id');


#################################


my $counter = 0;
while (my $dataset = <ANTENNA_INFO>) {

	$counter++;
	
	next if($dataset =~ m/^\D{2}/);		# get rid of the nonsense lines

	## 
	# extract the needed data
	my $antid = '';

	$_ = $dataset;
	($antid) = /^(\d{3})/;
	
	if( !defined($antid) || length($antid) != 3 ) {
		$XMLWRITER->dataElement("warning","Couldn't extract antenna id. Antenna id must be of 3 Characters length:\n\t(line $counter) -> $dataset", 'id' => 'Could not extract id');
		next;
	}
	

	##
	# check if the antid exists in the db
	if( !($ANTENNAS->{$antid}) ) {
		$XMLWRITER->dataElement("warning","An antenna with '$antid' does not exist in the database. It is not allowed to add antennas manually.");
		next;
	}
	
	##
	# update the database
	$UPDATE_ANT_STH->execute($antid, $antid)
					or &Exit("Could not execute statement: $DBI::errstr");
					
	if( $UPDATE_ANT_STH->rows == 0 ) {
		$XMLWRITER->dataElement("warning","Could not update $antid", 'id' => $antid);
	} else {
		#$XMLWRITER->dataElement("success","Updated successfully $antid", 'id' => $antid, );
	}

	
}

close(ANTENNA_INFO);




&Exit();

#########################################################
# SUBS
#########################################################

sub Exit {

	my $error = '';
	$error = shift;
	
	if( defined($DBH) ) {
		$DBH->disconnect();
	}
	
	if( defined($FILE) ) {
		&RemoveFile();
	}
	
	if( $error ne '') {
		$XMLWRITER->dataElement("error",$error);
	}
	
	##
	# feedback
	$XMLWRITER->endTag("feedback");
	$XMLWRITER->end();
	
	print $FEEDBACK;
	
	exit;
}

sub RemoveFile {
	if (-e $UPLOADDIR.$FILE) {	
		# remove file 
		if (unlink($UPLOADDIR.$FILE) == 0) {
			&Exit("Couldn't delete$UPLOADDIR$FILE");
		}	
	}
}