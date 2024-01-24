package Test::Dongry;
use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->parent->child ('lib');
use lib glob path (__FILE__)->parent->parent->parent->parent->child ('modules/*/lib');
use lib glob path (__FILE__)->parent->parent->parent->parent->child ('t_deps/lib');
use lib glob path (__FILE__)->parent->parent->parent->parent->child ('t_deps/modules/*/lib');
use AbortController;
use Web::Encoding;

use Exporter::Lite;
our @EXPORT;

use Test::MoreMore;
use Test::X1;
push @EXPORT, @Test::MoreMore::EXPORT;
push @EXPORT, @Test::X1::EXPORT;

require DBIx::ShowSQL;

use DongrySS;

#note "Servers...";
my $ac = AbortController->new;
my $v = DongrySS->run (
  signal => $ac->signal,
)->to_cv->recv;
our $ServerData = $v->{data};

## For Test::Class tests
push @EXPORT, qw(runtests);
sub runtests {
  eval {
    #note "Tests...";
    Test::Class->runtests (@_);
  };
  my $error;
  if ($@) {
    #note "Failed";
    warn $@;
    $error = 1;
  }
  
  #note "Done";
  $ac->abort;
  $v->{done}->to_cv->recv;
  undef $ac;
  undef $ServerData;
  undef $v;
  exit 1 if $error;
} # runtests

## For Test::X1 tests
push @EXPORT, qw(RUN);
sub RUN () {
  eval {
    #note "Tests...";
    run_tests;
  };
  my $error;
  if ($@) {
    #note "Failed";
    warn $@;
    $error = 1;
  }
  
  #note "Done";
  $ac->abort;
  $v->{done}->to_cv->recv;
  undef $ac;
  undef $ServerData;
  undef $v;
  exit 1 if $error;
} # RUN

my $DBNumber = 1;

push @EXPORT, qw(reset_db_set);
sub reset_db_set () {
  $DBNumber++;
} # reset_db_set

push @EXPORT, qw(test_dsn);
sub test_dsn ($;%) {
  my $name = shift || die;
  my %args = @_;

  my $dsn = {%{$ServerData->{local_dsn_options}->{root}}};
  my $test_dsn = $ServerData->{local_dsn_options}->{test};
  if ($name eq 'root') {
    $test_dsn = $dsn;
  }
  $name .= '_' . $DBNumber . '_test';
  
  my $client = AnyEvent::MySQL::Client->new;
  my %connect;
  if (defined $dsn->{port}) {
    $connect{hostname} = $dsn->{host}->to_ascii;
    $connect{port} = $dsn->{port};
  } else {
    $connect{hostname} = 'unix/';
    $connect{port} = $dsn->{mysql_socket};
  }
  $client->connect (
    %connect,
    username => $dsn->{user},
    password => $dsn->{password},
    database => $dsn->{dbname},
  )->then (sub {
    my $escaped = $dsn->{dbname} = $name . '_test';
    $escaped =~ s/`/``/g;
    return $client->query ("CREATE DATABASE IF NOT EXISTS `$escaped`")->then (sub {
      die $_[0] unless $_[0]->is_success;
      return $client->query (
        encode_web_utf8 sprintf q{grant all on `%s`.* to '%s'@'%s'},
        $escaped, $test_dsn->{user}, '%',
      );
    })->then (sub {
      die $_[0] unless $_[0]->is_success;
    });
  })->finally (sub {
    return $client->disconnect;
  })->to_cv->recv;

  if ($args{unix} or not $args{tcp}) {
    my $dsn = {%$test_dsn,
               dbname => $dsn->{dbname}};
    delete $dsn->{port};
    delete $dsn->{host};
    my $dsns = ServerSet->dsn ('mysql', $dsn);
    return $dsns;
  } else {
    my $dsn = {%$test_dsn,
               dbname => $dsn->{dbname}};
    delete $dsn->{mysql_socket};
    my $dsns = ServerSet->dsn ('mysql', $dsn);
    return $dsns;
  }
} # test_dsn

push @EXPORT, qw(new_db);
sub new_db (%) {
  my %args = @_;
  reset_db_set ();
  my $dsn = test_dsn ('test1');
  require Dongry::Database;
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}},
       schema => $args{schema});
  for my $name (keys %{$args{schema} || {}}) {
    if ($args{schema}->{$name}->{_create}) {
      $db->execute ($args{schema}->{$name}->{_create});
    }
  }
  if ($args{ae}) {
    $db->source (ae => {dsn => $dsn, writable => 1, anyevent => 1});
  }
  return $db;
} # new_db

1;

=head1 LICENSE

Copyright 2011-2024 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
