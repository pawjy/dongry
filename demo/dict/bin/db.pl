#!/usr/bin/perl
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->parent->parent->subdir ('lib')->stringify;
use lib glob file (__FILE__)->dir->parent->parent->parent->subdir ('modules', '*', 'lib')->stringify;
use Test::MySQL::CreateDatabase qw(
  test_dsn dsn2dbh copy_schema_from_file
);

sub prepare_db ($) {
  my $name = shift;
  my $dsn = test_dsn $name;
  my $schema_f = file (__FILE__)->dir->parent->subdir ('db')->file ($name . '.sql');
  my $dbh = dsn2dbh $dsn;
  copy_schema_from_file $schema_f => $dbh;
  return ($dsn, $dbh);
} # prepare_db

my ($dict_dsn) = prepare_db 'dict';

warn "Dict: $dict_dsn\n";

sleep 100 while 1;
