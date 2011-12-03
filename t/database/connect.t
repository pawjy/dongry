package test::Dongry::Database::connect;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;

my $test_dsn_1 = test_dsn ('hoge');

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

sub _source_from_new : Test(3) {
  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => 1}, fuga => {dsn => 2}});
  eq_or_diff $db->source ('hoge'), {dsn => 1};
  eq_or_diff $db->source ('fuga'), {dsn => 2};
  eq_or_diff $db->source ('foo'), undef;
} # _source_from_new

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

# XXX

# ------ |transaction| ------

# XXX

__PACKAGE__->runtests;

1;
