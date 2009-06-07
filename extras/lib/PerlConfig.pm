#!/usr/bin/perl

##############################################
# Main perl configuration and miscellaneous
# function to get configuration values from
# the XML confiuration file. 
#
# rleuthold@access.ch - 8.1.2009
##############################################

use strict;
use warnings;

use XML::Simple;
use Data::Dumper;

package PerlConfig;

#################################################################################################
# CONFIG
#################################################################################################

	##
	# Change the value for the XMLCONFIG constant to match the location of the XML configuration file
	use constant XMLCONFIG => '/var/www/mouse/conf/config.xml';

#################################
# CONSTRUCTOR
sub new {
	my $self = shift;
	
	# class attributes
	my %hash = (
						
		_XMLDATA	=> XML::Simple->new(
						ForceArray => ['grid'],
						KeepRoot => 0,
						KeyAttr => { grid => "-id" }
					)->XMLin(
						XMLCONFIG
					)
	);
	
	# constructor can be called by reference or name
	my $class = ref($self) || $self;
	return bless \%hash, $class;
}

###################################################
#  get the path to the configuration xml
sub get_config_xml_path {
	return XMLCONFIG;
}

#################################
# get a table name
sub get_dbname {

	my $self = shift @_;
	
	return $self->{_XMLDATA}->{db}->{dbname} ||
		die("Node <config><db><dbname> not found!");			
}

###################################################
#  get the database user
sub get_dbuser {
	my $self = shift @_;
	
	return $self->{_XMLDATA}->{db}->{dbuser} || 
		die("Node <config><db><dbuser> not found!\n");
}

###################################################
#  get the database user password
sub get_dbpass {
	my $self = shift @_;
	
	return $self->{_XMLDATA}->{db}->{dbpass} || 
		die("Node <config><db><dbpass> not found!\n");
}

###################################################
#  get the database name
sub get_dbsocket {
	my $self = shift @_;
	
	return $self->{_XMLDATA}->{db}->{dbsocket} || 
		die("Node <config><db><dbsocket> not found!\n");
}

###################################################
# get the folder with the perl import skripts 
sub get_scriptsfolder {

	my $self = shift @_;
	
	return $self->{_XMLDATA}->{perl}->{scriptsFolder} || 
		die("Element <config><perl><scriptsFolder> not found!\n");		
}


###################################################
# get interval for the direction and result search
sub get_antennainterval{

	my $self = shift @_;
	
	return $self->{_XMLDATA}->{perl}->{antennaInterval} || 
		die("Node <config><perl><antennaInterval> not found!\n");	
}

###################################################
# get user which runs the dbaackup 
sub get_dbbackupuser {

	my $self = shift @_;
	
	return $self->{_XMLDATA}->{perl}->{dbBackupUser} || 
		die("Node <config><perl><dbBackupUser> not found!\n");	
}

###################################################
# get directory to save the backup into 
sub get_dbbackupdirectory {

	my $self = shift @_;
	
	return $self->{_XMLDATA}->{perl}->{dbBackupDirectory} || 
		die("Node <config><perl><dbBackupDirectory> not found!\n");		
}


1;

