#!/usr/bin/perl

##############################################
# Get paths from the XML configuration file.
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

package XMLPaths;

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
						PerlConfig->new()->get_config_xml_path()
					)
	);
	
	# constructor can be called by reference or name
	my $class = ref($self) || $self;
	return bless \%hash, $class;
}

#################################
# dump the xml-configuration data
sub dump_xml_data {

	my $self = shift @_;
	print Data::Dumper->Dump([$self->{_XMLDATA}]);

}

#################################
# dump paths/directories
sub dump_paths {

	my $self = shift @_;
	my $directories = $self->{_XMLDATA}->{directories};
	print Data::Dumper->Dump([ $directories ]);

}

#################################
# Array ref with all path/directory element names
sub path_list {
	my $self = shift @_;
	return [keys %{ $self->{_XMLDATA}->{directories} }];
	
}

#################################
# get a path/directory
sub get_path {

	my ($self, $search_path) = @_;
	
	return $self->{_XMLDATA}->{directories}->{$search_path} || 
		die("Path element <$search_path> not found! Available path elements are:\n\t" . join("\n\t", map('<' . $_  .'>', @{ $self->path_list() }) ) . "\n");	
}

1;

