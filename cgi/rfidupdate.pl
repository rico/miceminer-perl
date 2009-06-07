#!/usr/bin/perl -w

##############################################################################
# This scripts imports a tab delimited file with the information for the rfids
# Line Format: rfid \t sex \t weight \t implant data
#
# rfid: 10 digits without (-) or spaces
# sex:  f/F/m/M
# weight: decimal number
# implant date: yyyy-mm-dd
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

my $TABLE_RFIDS = $TABLES->get_table_name('rfids'); # table rfids
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
my $XMLWRITER = new XML::Writer(OUTPUT => $FEEDBACK, NEWLINES => 0);
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
# getting rfids in the db
my $RFIDS_DB = $DBH->selectall_hashref("SELECT id, sex, weight, implant_date FROM $TABLE_RFIDS",'id');



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
open(RFID_INFO, "< $UPLOADDIR/$FILE") or &Exit("Could not open file '$FILE'");

##
# when the file is exported from excel it has ^M as the newline character ... replace them with a normal newline
##
open(RFID_CLEAN, "+> $UPLOADDIR/$FILE.tmp") or &Exit("Can't open file '$UPLOADDIR/$FILE.tmp': $!");

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


rename("$UPLOADDIR/$FILE.tmp", "$UPLOADDIR/$FILE");
open(RFID_INFO, "< $UPLOADDIR/$FILE") or &Exit("error","Can't open file '$UPLOADDIR/$FILE': $!");

#################################

my %RFIDS_UPDATED = ();


my $counter = 0;
while (my $dataset = <RFID_INFO>) {
	
	
	$counter++;
	
	next if($dataset =~ m/^\s+/);		# get rid of the nonsense lines
	next if($dataset !~ m/^\w{6,10}/);	# get rid of the nonsense lines

	#print $line ."\n";
	
	## 
	# extract the needed data
	my ($rfid, $sex, $weight, $implant_date) = ($dataset =~ /^(\w{6,10})\t(f|m)?\t(\d{1,2}\.?\d?)?\t(\d{4}\-\d{2}\-\d{2})?/i);

	#$XMLWRITER->dataElement("warning","(line $counter): $rfid", 'id' => "$rfid");
	#next;

	if( !defined($rfid) ) {
		$XMLWRITER->dataElement("error","Malformed line:\n\t(line $counter) -> $dataset\nPlease check the format of your values.", 'id' => "The line $counter seems to be malformed.");
		next;
	}
	
	$rfid = sprintf("%010s", $rfid);
	
	##
	# check if the rfid exists in the db
	if( !($RFIDS_DB->{$rfid}) ) {
		$XMLWRITER->dataElement("error","An rfid with id '$rfid' does not exist in the database.\n\t(line $counter) -> $dataset", 'id' => "$rfid does not exist");
		next;
	}
	
	# reformat rfid
	next if ($rfid eq '');
	$rfid =~ s/\-//g;
	
	# reformat sex
	$sex = " " if not defined $sex;
	$sex = lc($sex);
	
	# reformat weight
	if(defined $weight) {
		$weight = sprintf("%.1f", $weight);	
	} else {
		$weight = undef;
	}
	
	# reformat implant_date
	$implant_date = undef if not defined $implant_date;

	
	$RFIDS_UPDATED{$rfid} = 
						{
							'sex' => $sex,
							'weight' => $weight,
						 	'implant_date' => $implant_date
						};
	#exit;

	
}

#print " => \$RFIDS_UPDATED: " . Dumper(\%RFID_INFO) ."\n";
#exit;
#print"Counter: $counter\n";
close(RFID_INFO);

##
# Updating rfid db table
####################################
my $UPDATE_RFID_STH = $DBH->prepare(qq{UPDATE $TABLE_RFIDS SET sex = ?, weight = ?, implant_date = ? WHERE id = ?}) 
									or &Exit("Could not prepare statement to update $TABLE_RFIDS table: " . $DBH->errstr);


foreach my $rfid_updated_id (keys %RFIDS_UPDATED) {
	
	##
	# Getting actual values
	my $db_values = $RFIDS_DB->{$rfid_updated_id};
	
	##
	# updated values
	my  $values = $RFIDS_UPDATED{$rfid_updated_id};
	
	my $update = 0;
	
	##
	# checking if nothing is set to a zero variable
	if( defined($db_values->{'sex'}) ) {
		if( ($values->{'sex'} eq " ") && ($db_values->{'sex'} =~ /(f|m)/) ) {
			$XMLWRITER->dataElement("warning","The value for rfid $rfid_updated_id sex in your file is empty whereas in the database it was set to " . $db_values->{'sex'} . ". The value for 'sex' has been cleared" , 'id' => "rfid $rfid_updated_id value sex cleared");
			$update = 1;
		}
	} elsif( $values->{'sex'} =~ /(f|m)/ ) {
		$update = 1;
	}
	
	##
	# weight
	if( !defined $values->{'weight'} && defined $db_values->{'weight'} ) {
		$XMLWRITER->dataElement("warning","The value for rfid $rfid_updated_id weight in your file is empty  whereas in the database it was set to " . $db_values->{'weight'} . ". The value for 'weight' has been cleared." , 'id' => "rfid $rfid_updated_id value weight cleared");
		$update = 1;
	}
	
	if( $values->{'weight'} =~ /\d{1,2}\.\d/ ) {
		$update = 1;
	}

	##
	# implant date
	if( !defined $values->{'implant_date'} && defined $db_values->{'implant_date'} ) {
		$XMLWRITER->dataElement("warning","The value for rfid $rfid_updated_id implant date in your file is empty  whereas in the database it was set to " . $db_values->{'implant_date'} . ". The value for implant date' has been cleared." , 'id' => "rfid $rfid_updated_id value implant date cleared");
		$update = 1;
	}
	
	if( $values->{'implant_date'} =~ /\d{4}\-\d{2}\-\d{2}/ ) {
		$update = 1;
	}
	
	
	##
	# update only if there is something to update
	if($update) {
	
		$UPDATE_RFID_STH->execute($values->{'sex'}, $values->{'weight'}, $values->{'implant_date'}, $rfid_updated_id  )
						or &Exit("Could not execute statement: $DBI::errstr");
						
		if( $UPDATE_RFID_STH->rows == 0 ) {
			$XMLWRITER->dataElement("error","Could not update $rfid_updated_id", 'id' => $rfid_updated_id);
		} else {
			#$XMLWRITER->dataElement("success","Updated successfully", 'id' => $rfid_updated_id,);
		}
	}

}


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