#!/usr/bin/perl

##############################################
# Get database tables from the XML configuration file.
#
# !! The path to the XML configuration file must be
# set on line 34. !!
#
# rleuthold@access.ch - 8.1.2009
##############################################

use strict;
use warnings;

use XML::Simple;
use Data::Dumper;
use PerlConfig;

package DBTables;
use base qw(PerlConfig);

#################################
# CONSTRUCTOR
#################################
# CONSTRUCTOR
sub new {
	my $self = shift;
	
	# class attributes
	my %hash = (
		XMLDATA	=> XML::Simple->new(
						ForceArray => ['table'],
						KeepRoot => 0,
						KeyAttr => { table => "+id" }
					)->XMLin(
						PerlConfig->new()->get_config_xml_path()
					),
		DAYS_TO_COUNT_TABLE => 'days_to_count'
	);
	
	# constructor can be called by reference or name
	my $class = ref($self) || $self;
	return bless \%hash, $class;
}

#################################
# dump tables
sub dump_tables {

	my $self = shift @_;
	my $tables = $self->{XMLDATA}->{db}->{tables};
	print Data::Dumper->Dump([ $tables ]);

}

#################################
# Array ref with all path/directory element names
sub get_tables_list {
	my $self = shift @_;
	return [keys %{ $self->{XMLDATA}->{db}->{tables}->{table} }];
	
}

#################################
# get days to count table name
# this table is not listed in the configuration xml
# cause it's used within the scripts only
sub get_days_to_count_table {
	my $self = shift @_;
	return $self->{DAYS_TO_COUNT_TABLE};
}

#################################
# get a table name
sub get_table_name {

	my ($self, $search_table) = @_;
	
	if(defined $self->{XMLDATA}->{db}->{tables}->{table}->{$search_table}->{content}) {
		return $self->{XMLDATA}->{db}->{tables}->{table}->{$search_table}->{content};
	} else { 
		die("Table element <$search_table> not found!");
	}
	 
			
}

1;

