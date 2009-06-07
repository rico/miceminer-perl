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

my $TABLE_BOXES = $TABLES->get_table_name('boxes');	# table boxes
my $UPLOADDIR 	= $PATHS->get_path('data');			# upload directory

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

##
# getting boxes in the db
my $BOXES_DB = $DBH->selectall_hashref("SELECT id, xcoord, ycoord, segment FROM $TABLE_BOXES",'id');

# print " => \$BOXES_DB: " . Dumper($BOXES_DB) ."\n";
# exit;

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
open(BOX_INFO, "< $UPLOADDIR/$FILE") or &Exit("Could not open file '$FILE'");

##
# when the file is exported from excel it has ^M as the newline character ... replace them with a normal newline
##
open(BOX_CLEAN, "+> $UPLOADDIR/$FILE.tmp") or &Exit("Can't open file '$UPLOADDIR/$FILE.tmp': $!");

while (my $line = <BOX_INFO>) {
	
	if($line =~ m/\x0D/) {
		$line =~ s/^\s+//g;	
		chomp($line);
		$line =~ s/\x0D/\n/g;	
		print BOX_CLEAN $line;
	} else {
		$line =~ s/^\s+//g;	
		print BOX_CLEAN $line;
	}
}

close(BOX_CLEAN);
close(BOX_INFO);


rename("$UPLOADDIR/$FILE.tmp", "$UPLOADDIR/$FILE");
open(BOX_INFO, "< $UPLOADDIR/$FILE") or &Exit("error","Can't open file '$UPLOADDIR/$FILE': $!");

#################################

my %BOXES_UPDATED = ();


my $counter = 0;
while (my $dataset = <BOX_INFO>) {

	$counter++;
	
	next if($dataset =~ m/^\D{2}/);		# get rid of the nonsense lines
	#print $dataset;
	$_ = $dataset;

	
	

	my ($boxid, $xcoord, $ycoord, $segment, $tab3) = ('','','','');

	($boxid, $xcoord, $ycoord, $segment) = ($dataset =~ /^(\d{1,2})\t(\d+)?\t(\d+)?\t(a|b|c|d)?/i);
	
	$xcoord = 0 if not defined $xcoord;
	$ycoord = 0 if not defined $ycoord;
	$segment = '' if not defined $segment;
	$segment =~ s//\t/g;
	$segment = lc($segment);
	
	$boxid = sprintf('%02s', $boxid);
	
	if( !defined($boxid) ) {
		$XMLWRITER->dataElement("error","Couldn't extract box id. Box id must be of 2 Characters length:\n\t(line $counter) -> $dataset", 'id' => "no box id found");
		next;
	}
	
	##
	# check if the boxid exists in the db
	if( !($BOXES_DB->{$boxid}) ) {
		$XMLWRITER->dataElement("error","A box with id '$boxid' does not exist in the database.\n\t(line $counter) -> $dataset", 'id' => "Box '$boxid' does not exist");
		next;
	}
	

	#$XMLWRITER->dataElement("success","boxid: $boxid \n xccord: $xcoord \n ycoord: $ycoord \n segment: $segment \n -> line: $dataset", 'id' => "$boxid");
	#next;
	
	#print"$box\t$sex\t$weight\t$implant_date\n";
	
	$BOXES_UPDATED{$boxid} = 
					{	
						'xcoord' => $xcoord,
						'ycoord' => $ycoord,
						'segment' => $segment
					};
	#exit;

	
}

#print " => \$BOXES_UPDATED: " . Dumper(\%BOX_INFO) ."\n";
#exit;
#print"Counter: $counter\n";
close(BOX_INFO);

##
# Updating box db table
####################################
my $UPDATE_BOX_STH = $DBH->prepare(qq{UPDATE $TABLE_BOXES set xcoord = ?, ycoord = ?, segment = ? WHERE id = ?}) 
									or &Exit("Could not prepare statement to update $TABLE_BOXES table: " . $DBH->errstr);

foreach my $box_updated_id (keys %BOXES_UPDATED) {
	
	##
	# Getting actual values
	my $db_values = $BOXES_DB->{$box_updated_id};
	
	##
	# getting update values
	my  $values = $BOXES_UPDATED{$box_updated_id};
	
	my $update = 0;
	
	##
	# checking if nothing is set to a zero variable
	if( defined($db_values->{'xcoord'}) && $values->{'xcoord'} == 0 && $db_values->{'xcoord'} != 0) {
		$values->{'xcoord'} = $db_values->{'xcoord'};
		$XMLWRITER->dataElement("warning","The value for box $box_updated_id 'xcoord' in your file is empty or unrecognized whereas in the database it is set to " . $db_values->{'xcoord'} . ". The xcoord value remains at " . $db_values->{'xcoord'} . "." , 'id' => "Box $box_updated_id value 'xcoord' not updated ");
	} elsif( $values->{'xcoord'} != $db_values->{'xcoord'} ) {
		$update = 1;
	}
	
	if( defined($db_values->{'ycoord'}) && $values->{'ycoord'} == 0 && $db_values->{'ycoord'} != 0) {
		$values->{'ycoord'} = $db_values->{'ycoord'};
		$XMLWRITER->dataElement("warning","The value for box $box_updated_id 'ycoord' in your file is empty or unrecognized whereas in the database it is set to " . $db_values->{'ycoord'} . ". The ycoord value remains at " . $db_values->{'ycoord'} . "." , 'id' => "Box $box_updated_id value 'ycoord' not updated ");
	} elsif( $values->{'ycoord'} != $db_values->{'ycoord'} ) {
		$update = 1;
	} 
	
	if( defined($db_values->{'segment'}) && $values->{'segment'} eq '' && $db_values->{'segment'} ne '') {
		$values->{'segment'} = $db_values->{'segment'};
		$XMLWRITER->dataElement("warning","The value for box $box_updated_id 'segment' in your file is empty or unrecognized whereas in the database it is set to " . $db_values->{'segment'} . ". The segment value remains at " . $db_values->{'segment'} . "." , 'id' => "Box $box_updated_id value 'segment' not updated ");
	} elsif( $values->{'segment'} ne $db_values->{'segment'} ) {
		$update = 1;
	}
	
	
	
	##
	# updating the data
	if($update) {
		$UPDATE_BOX_STH->execute($values->{'xcoord'}, $values->{'ycoord'}, $values->{'segment'}, $box_updated_id )
						or &Exit("Could not execute statement: $DBI::errstr");
						
		if( $UPDATE_BOX_STH->rows == 0 ) {
			$XMLWRITER->dataElement("error","Could not update $box_updated_id", 'id' => $box_updated_id);
		} else {
			#$XMLWRITER->dataElement("success","Updated successfully", 'id' => $box_updated_id, );
		}
	}
}


&Exit();

#########################################################
# SUBS
#########################################################

sub Exit {

	my $error = shift;
	
	if( defined($DBH) ) {
		$DBH->disconnect();
	}
	
	if( defined($FILE) ) {
		&RemoveFile();
	}
	
	if( defined($error) ) {
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
			&Exit("Couldn't delete" . $UPLOADDIR.$FILE);
		}	
	}
}