use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t/lib');
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
use Test::X1;
use Test::Dongry;
use Dongry::Database;

my $dsn = test_dsn 'hoge1';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn}});

  my $invoked = 0;
  $db->connect ('ae', cb => sub {
    my ($db0, $result) = @_;
    test {
      is $db0, $db;
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
    $invoked++;
    $_[0]->disconnect ('ae', cb => sub {
      test {
        is $invoked, 1;
      } $c;
    });
  });

  is $invoked, 1;
  done $c;
} n => 12, name => 'connect cb';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn}});

  my $invoked = 0;
  my $result = $db->connect ('ae', cb => sub {
    my ($db0, $result) = @_;
    test {
      is $db0, $db;
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
    $invoked++;
    $_[0]->disconnect ('ae', cb => sub {
      test {
        is $invoked, 1;
      } $c;
    });
  });

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok not $result->can ('then');
  dies_here_ok {
    $result->then;
  };

  done $c;
} n => 15, name => 'connect return';

test {
  my $c = shift;
  my $db = Dongry::Database->new;
  my $result = $db->disconnect;
  isa_ok $result, 'Dongry::Database::Executed';
  ok not $result->can ('then');
  done $c;
} n => 2, name => 'disconnect no source';

test {
  my $c = shift;
  my $db = Dongry::Database->new (sources => {sync => {dsn => ''}});
  my $result = $db->disconnect;
  isa_ok $result, 'Dongry::Database::Executed';
  ok not $result->can ('then');
  done $c;
} n => 2, name => 'disconnect sync source only';

run_tests;

=head1 LICENSE

Copyright 2011-2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
