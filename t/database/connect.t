package test::Dongry::Database::connect;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;

my $test_dsn_1 = test_dsn ('hoge');
my $test_dsn_2 = test_dsn ('fuga');

# ------ |source| ------

sub _source_default : Test(6) {
  my $db = Dongry::Database->new;

  eq_or_diff $db->source, undef;
  eq_or_diff $db->source ('default'), undef;

  $db->source (undef, {hoge => 1, abc => 2});
  eq_or_diff $db->source, {hoge => 1, abc => 2};
  eq_or_diff $db->source ('default'), {hoge => 1, abc => 2};

  $db->source ('default', {hoge => 3, fuga => 2});
  eq_or_diff $db->source, {hoge => 3, fuga => 2};
  eq_or_diff $db->source ('default'), {hoge => 3, fuga => 2};
} # _source_default

sub _source_named : Test(4) {
  my $db = Dongry::Database->new;
  
  eq_or_diff $db->source ('heavy'), undef;
  
  $db->source (heavy => {dsn => 'foo'});
  eq_or_diff $db->source ('heavy'), {dsn => 'foo'};

  $db->source (select => {dsn => 'abc'});
  eq_or_diff $db->source ('heavy'), {dsn => 'foo'};
  eq_or_diff $db->source ('select'), {dsn => 'abc'};
} # _source_named

sub _source_from_new_default : Test(3) {
  my $db = Dongry::Database->new
      (sources => {default => {dsn => 1}, fuga => {dsn => 2}});
  eq_or_diff $db->source ('default'), {dsn => 1};
  eq_or_diff $db->source ('fuga'), {dsn => 2};
  eq_or_diff $db->source ('foo'), undef;
} # _source_from_new_default

sub _source_from_new : Test(3) {
  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => 1}, fuga => {dsn => 2}});
  eq_or_diff $db->source ('hoge'), {dsn => 1};
  eq_or_diff $db->source ('fuga'), {dsn => 2};
  eq_or_diff $db->source ('foo'), undef;
} # _source_from_new

# ------ |onconnect| ------

sub _onconnect_new : Test(2) {
  my ($onconnect_self, %onconnect_args);
  my $db = Dongry::Database->new (onconnect => sub {
    ($onconnect_self, %onconnect_args) = @_;
  });
  $db->source (hoge => {
    dsn => $test_dsn_1,
  });
  lives_ok { $db->connect ('hoge') };
  is $onconnect_self, $db;
} # _onconnect_new

# ------ |onerror| ------

sub _onerror_new : Test(2) {
  my ($onerror_self, %onerror_args);
  my $db = Dongry::Database->new (onerror => sub {
    ($onerror_self, %onerror_args) = @_;
  });
  $db->source (hoge => {
    dsn => $test_dsn_1,
    password => 'foo',
  });
  dies_ok { $db->connect ('hoge') };
  is $onerror_self, $db;
} # _onerror_new

# ------ |connect| ------

sub _connect_without_name : Test(1) {
  my $db = Dongry::Database->new;
  dies_ok { $db->connect };
} # _connect_without_name

sub _connect_with_unknown_name : Test(1) {
  my $db = Dongry::Database->new;
  dies_ok { $db->connect ('unknown') };
} # _connect_with_unknown_name

sub _connect_unknown_name_onerror : Test(2) {
  my $db = Dongry::Database->new;
  my ($onerror_self, %onerror_args);
  $db->onerror (sub {
    ($onerror_self, %onerror_args) = @_;
  });
  dies_ok { $db->connect ('hoge') };
  is $onerror_self, undef;
} # _connect_unknown_name_onerror

sub _connect_unknown_ds : Test(1) {
  my $db = Dongry::Database->new;
  $db->source (hoge => {
    dsn => 'bad dsn',
  });
  dies_ok { $db->connect ('hoge') };
} # _connect_with_unknown_ds

sub _connect_with_unknown_ds_onerror : Test(2) {
  my $db = Dongry::Database->new;
  my ($onerror_self, %onerror_args);
  $db->onerror (sub {
    ($onerror_self, %onerror_args) = @_;
  });
  $db->source (hoge => {
    dsn => 'bad dsn',
  });
  dies_ok { $db->connect ('hoge') };
  is $onerror_self, undef;
} # _connect_with_unknown_ds_onerror

sub _connect_wrong_mysql_dsn_onerror : Test(5) {
  my $db = Dongry::Database->new;
  my $dsn = $test_dsn_1;
  $dsn =~ s/\buser=/x-user=/g;
  my ($onerror_self, %onerror_args);
  $db->onerror (sub {
    ($onerror_self, %onerror_args) = @_;
  });
  $db->source (hoge => {
    dsn => 'dbi:mysql:bad dsn',
  });
  dies_ok { $db->connect ('hoge') };
  is $onerror_self, $db;
  is $onerror_args{source_name}, 'hoge';
  like $onerror_args{text}, qr{DBI connect.*failed};
  is $onerror_args{sql}, undef;
} # _connect_wrong_mysql_dsn_onerror

sub _connect_connected : Test(2) {
  my $db = Dongry::Database->new;
  $db->source (hoge => {
    dsn => $test_dsn_1,
  });
  lives_ok { $db->connect ('hoge') };
  lives_ok { $db->connect ('hoge') };
} # _connect_connected

sub _connect_wrong_username : Test(1) {
  my $db = Dongry::Database->new;
  my $dsn = $test_dsn_1;
  $dsn =~ s/\buser=/x-user=/g;
  $db->source (hoge => {
    dsn => $dsn,
    username => 'hoge',
  });
  dies_ok { $db->connect ('hoge') };
} # _connect_wrong_username

sub _connect_wrong_username_onerror : Test(5) {
  my $db = Dongry::Database->new;
  my $dsn = $test_dsn_1;
  $dsn =~ s/\buser=/x-user=/g;
  my ($onerror_self, %onerror_args);
  $db->onerror (sub {
    ($onerror_self, %onerror_args) = @_;
  });
  $db->source (hoge => {
    dsn => $dsn,
    username => 'hoge',
  });
  dies_ok { $db->connect ('hoge') };
  is $onerror_self, $db;
  is $onerror_args{source_name}, 'hoge';
  like $onerror_args{text}, qr{DBI connect.*failed};
  is $onerror_args{sql}, undef;
} # _connect_wrong_username

sub _connect_wrong_password : Test(1) {
  my $db = Dongry::Database->new;
  $db->source (hoge => {
    dsn => $test_dsn_1,
    password => 'hoge',
  });
  dies_ok { $db->connect ('hoge') };
} # _connect_wrong_password

sub _connect_wrong_password_onerror : Test(5) {
  my $db = Dongry::Database->new;
  my ($onerror_self, %onerror_args);
  $db->onerror (sub {
    ($onerror_self, %onerror_args) = @_;
  });
  $db->source (hoge => {
    dsn => $test_dsn_1,
    password => 'hoge',
  });
  dies_ok { $db->connect ('hoge') };
  is $onerror_self, $db;
  is $onerror_args{source_name}, 'hoge';
  like $onerror_args{text}, qr{DBI connect.*failed};
  is $onerror_args{sql}, undef;
} # _connect_wrong_password

sub _connect_onconnect : Test(3) {
  my $invoked = 0;
  my ($onconnect_self, %onconnect_args);
  my $db = Dongry::Database->new;
  $db->source (hoge => {
    dsn => $test_dsn_1,
  });
  $db->onconnect (sub {
    ($onconnect_self, %onconnect_args) = @_;
    $invoked++;
  });
  $db->connect ('hoge');
  $db->connect ('hoge');

  is $invoked, 1;
  is $onconnect_self, $db;
  is $onconnect_args{source_name}, 'hoge';
} # _connect_onconnect

sub _connect_onconnct_after_disconnect : Test(3) {
  my $invoked = 0;
  my ($onconnect_self, %onconnect_args);
  my $db = Dongry::Database->new;
  $db->source (hoge => {
    dsn => $test_dsn_1,
  });
  $db->onconnect (sub {
    ($onconnect_self, %onconnect_args) = @_;
    $invoked++;
  });
  $db->connect ('hoge');
  $db->disconnect ('hoge');
  $db->connect ('hoge');

  is $invoked, 2;
  is $onconnect_self, $db;
  is $onconnect_args{source_name}, 'hoge';
} # _connect_onconnect

# ------ |disconnect| ------

sub _disconnect_disconnected : Test(3) {
  my $db = Dongry::Database->new;
  $db->source (hoge => {dsn => $test_dsn_1});
  $db->connect ('hoge');
  ok $db->{dbhs}->{hoge};
  
  lives_ok { $db->disconnect ('hoge') };
  ng $db->{dbhs}->{hoge};
} # _disconnect_disconnected

sub _disconnect_nop : Test(2) {
  my $db = Dongry::Database->new;
  $db->source (hoge => {dsn => $test_dsn_1});
  
  lives_ok { $db->disconnect ('hoge') };
  ng $db->{dbhs}->{hoge};
} # _disconnect_nop

sub _disconnect_unknown_source : Test(2) {
  my $db = Dongry::Database->new;
  
  lives_ok { $db->disconnect ('fuga') };
  ng $db->{dbhs}->{fuga};
} # _disconnect_unknown_source

sub _disconnect_disconnected_some : Test(5) {
  my $db = Dongry::Database->new;
  $db->source (hoge => {dsn => $test_dsn_1});
  $db->source (fuga => {dsn => $test_dsn_2});
  $db->connect ('hoge');
  $db->connect ('fuga');
  ok $db->{dbhs}->{hoge};
  ok $db->{dbhs}->{fuga};
  
  lives_ok { $db->disconnect ('hoge') };
  ng $db->{dbhs}->{hoge};
  ok $db->{dbhs}->{fuga};
} # _disconnect_disconnected_some

sub _disconnect_all_disconnected : Test(3) {
  my $db = Dongry::Database->new;
  $db->source (hoge => {dsn => $test_dsn_1});
  $db->connect ('hoge');
  ok $db->{dbhs}->{hoge};
  
  lives_ok { $db->disconnect };
  ng $db->{dbhs}->{hoge};
} # _disconnect_disconnected

sub _disconnect_all_disconnected_multiple : Test(5) {
  my $db = Dongry::Database->new;
  $db->source (hoge => {dsn => $test_dsn_1});
  $db->source (fuga => {dsn => $test_dsn_2});
  $db->connect ('hoge');
  $db->connect ('fuga');
  ok $db->{dbhs}->{hoge};
  ok $db->{dbhs}->{fuga};
  
  lives_ok { $db->disconnect };
  ng $db->{dbhs}->{hoge};
  ng $db->{dbhs}->{fuga};
} # _disconnect_all_disconnected

sub _disconnect_all_disconnected_nop : Test(2) {
  my $db = Dongry::Database->new;
  
  lives_ok { $db->disconnect };
  ng $db->{dbhs}->{hoge};
} # _disconnect_disconnected

{
  package test::destroy::1;
  sub DESTROY {
    $test::destroy::1::destroyed++;
  }
}

sub _destroy : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'hoge';
  my $db = Dongry::Database->new;
  $db->source (default => {dsn => $dsn});
  my $result = $db->execute ('show tables');
  $db->{dummy} = bless {}, 'test::destroy::1';
  $test::destroy::1::destroyed = 0;
  undef $db;
  is $test::destroy::1::destroyed, 0;
  undef $result;
  is $test::destroy::1::destroyed, 1;
} # _destroy

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
