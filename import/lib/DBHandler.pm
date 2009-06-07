#!/usr/bin/perl

#
# Database connection class
################################

use strict;
use warnings;


use DBI;
use DBI qw(:sql_types);
use Data::Dumper;
use PerlConfig;

package DBHandler;

#################
# CONSTRUCTOR
sub new {
	my $self = shift;
	my $PERLCONFIG = PerlConfig->new();
	# object attributes
	my %hash = (
		DB => $PERLCONFIG->get_dbname(),
		_DBUSER => $PERLCONFIG->get_dbuser(),
		_DBPASS => $PERLCONFIG->get_dbpass(),
		_SOCKET	=> $PERLCONFIG->get_dbsocket(),
		DBH		=> undef,
	);
	
	# constructor can be called by reference or name
	my $class = ref($self) || $self;
	return bless \%hash, $class;
}


#################
# connect to db
sub connect {
	my ($self) = shift(@_);
	
	if($self eq __PACKAGE__) {
		$self = new(__PACKAGE__);
	}
	
	my $dbh = DBI->connect('DBI:mysql:' . $self->{DB} . ';mysql_socket=' . $self->{_SOCKET}, "$self->{_DBUSER}", "$self->{_DBPASS}", 
			{ RaiseError => 0, AutoCommit => 0 })
		or die ("Could not connect to database: $DBI::errstr");
		
		
	return $self->{DBH} = $dbh;
}


#################
# disconnect from db
sub disconnect {
	my ($self) = shift(@_);
	
	$self->{DBH}->disconnect();

}

1;

