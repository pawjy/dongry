package test::Dongry::Database::anyevent::connect;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use AnyEvent;

# ------ connect ------

sub _connect_ae_created : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';

  my $cv = AnyEvent->condvar;

  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => $dsn, anyevent => 1}});
  $db->connect ('hoge');
  isa_ok $db->{dbhs}->{hoge}, 'AnyEvent::MySQL::db';

  $db->execute ('show tables', undef, source_name => 'hoge', cb => sub {
    $cv->send;
  });
  
  $cv->recv;
  ok 1;
} # _connect_ae_created

sub _connect_ae_created_cb : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'hoge1';

  my $cv = AnyEvent->condvar;

  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => $dsn, anyevent => 1}});
  $db->onconnect (sub {
    isa_ok $db->{dbhs}->{hoge}, 'AnyEvent::MySQL::db';

    $db->execute ('show tables', undef, source_name => 'hoge', cb => sub {
      $cv->send;
    });
  });
  $db->connect ('hoge');
  
  $cv->recv;
  ok 1;
} # _connect_ae_created_cb

sub _connect_ae_dsn_error : Test(5) {
return;# XXX
  my $cv = AnyEvent->condvar;

  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => 'dbi:mysql:host=notfound', anyevent => 1}});
  $db->connect ('hoge');
  isa_ok $db->{dbhs}->{hoge}, 'AnyEvent::MySQL::db';

  my $success;
  my $error;
  $cv->begin;

  $cv->begin;
  $db->execute ('show tables', undef, source_name => 'hoge', cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->end;
  });

  $cv->begin;
  $db->execute ('select * from foo', undef, source_name => 'hoge',
                cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->end;
  });

  $cv->end;

  $cv->recv;
  ng $success;
  is $error, 2;

  $cv = AnyEvent->condvar;
  $db->execute ('select * from foo', undef, source_name => 'hoge', 
                cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->send;
  });

  $cv->recv;
  ng $success;
  is $error, 3;
} # _connect_ae_dsn_error

sub _connect_ae_dsn_error_with_onerror : Test(9) {
return; # XXX
  my $cv = AnyEvent->condvar;

  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => 'dbi:mysql:notfound', anyevent => 1}});

  my @onerror;
  $db->onerror (sub {
    my (undef, %args) = @_;
    push @onerror, \%args;
  });

  $db->connect ('hoge');
  my $error_line = __LINE__ - 1;
  isa_ok $db->{dbhs}->{hoge}, 'Dongry::Database::BrokenConnection';

  my $success;
  my $error;
  $cv->begin;

  $cv->begin;
  $db->execute ('show tables', undef, source_name => 'hoge', cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->end;
  });

  $cv->begin;
  $db->execute ('select * from foo', undef, source_name => 'hoge',
                cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->end;
  });

  $cv->end;

  $cv->recv;
  ng $success;
  is $error, 2;

  $cv = AnyEvent->condvar;
  $db->execute ('select * from foo', undef, source_name => 'hoge', 
                cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->send;
  });

  $cv->recv;
  ng $success;
  is $error, 3;

  $cv = AE::cv;
  AE::postpone {
    is scalar @onerror, 1;
    like $onerror[0]->{text}, qr{Can't connect|Unknown database|Access denied for user|invalid dsn format};
    is $onerror[0]->{file_name}, __FILE__;
    is $onerror[0]->{line}, $error_line;
    $cv->send;
  };
  $cv->recv;
} # _connect_ae_dsn_error_with_onerror

sub _connect_ae_dsn_error_with_onerror_implied_connect : Test(9) {
return; # XXX
  my $cv = AnyEvent->condvar;

  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => 'dbi:mysql:notfound', anyevent => 1}});

  my @onerror;
  $db->onerror (sub {
    my (undef, %args) = @_;
    push @onerror, \%args;
  });

  my $success;
  my $error;
  $cv->begin;

  $cv->begin;
  $db->execute ('show tables', undef, source_name => 'hoge', cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->end;
  });
  my $error_line = __LINE__ - 1;
  isa_ok $db->{dbhs}->{hoge}, 'Dongry::Database::BrokenConnection';

  $cv->begin;
  $db->execute ('select * from foo', undef, source_name => 'hoge',
                cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->end;
  });

  $cv->end;

  $cv->recv;
  ng $success;
  is $error, 2;

  $cv = AnyEvent->condvar;
  $db->execute ('select * from foo', undef, source_name => 'hoge', 
                cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->send;
  });

  $cv->recv;
  ng $success;
  is $error, 3;

  $cv = AE::cv;
  AE::postpone {
    is scalar @onerror, 1;
    like $onerror[0]->{text}, qr{Can't connect|Unknown database|Access denied for user|invalid dsn format};
    is $onerror[0]->{file_name}, __FILE__;
    is $onerror[0]->{line}, $error_line;
    $cv->send;
  };
  $cv->recv;
} # _connect_ae_dsn_error_with_onerror_implied_connect

sub _connect_ae_dsn_error_with_onerror_die : Test(9) {
return; # XXX
  my $cv = AnyEvent->condvar;

  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => 'dbi:mysql:notfound', anyevent => 1}});

  my @onerror;
  $db->onerror (sub {
    my (undef, %args) = @_;
    push @onerror, \%args;
    die 'test';
  });

  $db->connect ('hoge');
  my $error_line = __LINE__ - 1;
  isa_ok $db->{dbhs}->{hoge}, 'Dongry::Database::BrokenConnection';

  my $success;
  my $error;
  $cv->begin;

  $cv->begin;
  $db->execute ('show tables', undef, source_name => 'hoge', cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->end;
  });

  $cv->begin;
  $db->execute ('select * from foo', undef, source_name => 'hoge',
                cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->end;
  });

  $cv->end;

  $cv->recv;
  ng $success;
  is $error, 2;

  $cv = AnyEvent->condvar;
  $db->execute ('select * from foo', undef, source_name => 'hoge', 
                cb => sub {
    $_[1]->is_success ? $success++ : $error++;
    $cv->send;
  });

  $cv->recv;
  ng $success;
  is $error, 3;

  $cv = AE::cv;
  AE::postpone {
    is scalar @onerror, 1;
    like $onerror[0]->{text}, qr{Can't connect|Unknown database|Access denied for user|invalid dsn format};
    is $onerror[0]->{file_name}, __FILE__;
    is $onerror[0]->{line}, $error_line;
    $cv->send;
  };
  $cv->recv;
} # _connect_ae_dsn_error_with_onerror_die

sub _connect_onconnect : Test(2) {
  my $cv = AnyEvent->condvar;

  reset_db_set;
  my $dsn = test_dsn 'fuga';
  
  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => $dsn, anyevent => 1}});
  my $db2;

  my @connected;
  $db->onconnect (sub {
    my ($self, %args) = @_;
    $db2 = ''.$self;
    push @connected, $args{source_name};
  });

  $db->connect ('hoge');
  $db->connect ('hoge');

  my $timer; $timer = AE::timer 3, 0, sub {
    eq_or_diff \@connected, ['hoge'];
    is $db2, ''.$db;
    $cv->send;
    undef $timer;
  };
  $cv->recv;
} # _connect_onconnect

sub _connect_onconnect_timing : Test(2) {
  my $cv = AnyEvent->condvar;
  $cv->begin;

  reset_db_set;
  my $dsn = test_dsn 'fuga';
  
  my $db = Dongry::Database->new
      (sources => {hoge => {dsn => $dsn, anyevent => 1, writable => 1},
                   sync => {dsn => $dsn, writable => 1}});
#  $db->onconnect (sub {
#    my ($db, %args) = @_;
#    $db->execute ('select * from foo where id = 1', undef,
#                  source_name => 'sync', cb => sub {
#    })
#  });
  $db->execute ('create table foo (id int, value blob)', undef,
                source_name => 'sync');
  $db->execute ('insert into foo (id, value) values (1, "abc")', undef,
                source_name => 'sync');

  my $cv2 = AE::cv;
  $cv->begin;
  my $value1;
  $db->onconnect (sub {
    my ($db, %args) = @_;
    $db->execute ('select * from foo where id = 1', undef,
                  source_name => 'hoge', cb => sub {
      $value1 = $_[1]->first->{value};
      $cv->end;
      $cv2->send;
    });
  });

  $db->execute ('update foo set value = "xyz" where id = 1', undef,
                source_name => 'hoge', cb => sub { warn "update cb" });
  $cv2->recv;
  
  my $value2;
  $db->execute ('select * from foo where id = 1', undef,
                source_name => 'hoge', cb => sub {
    $value2 = $_[1]->first->{value};
  });

  $cv->begin;
  $db->execute ('select 1', undef, source_name => 'hoge', cb => sub {
    $cv->end;
  });

  $cv->end;
  $cv->recv;

  is $value1, 'abc';
  is $value2, 'xyz';
} # _connect_onconnect_timing

# ------ disconnect ------

sub _disconnect_unused : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge';
  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1}});

  $db->connect ('ae');

  lives_ok { $db->disconnect };
} # _disconnect_unused

sub _disconnect_used : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'hoge';
  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1}});

  my $cv = AnyEvent->condvar;

  $db->execute ('show tables', undef, cb => sub {
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  lives_ok { $db->disconnect };
  lives_ok { $db->disconnect };
} # _disconnect_used

# ------ destroy ------

{
  package test::destroy::1;
  sub DESTROY {
    $test::destroy::1::destroyed++;
  }
}

sub _destroy_connected : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge';
  my $db = Dongry::Database->new;
  $db->source (default => {dsn => $dsn, anyevent => 1});
  $db->connect ('default');
  $db->{dummy} = bless {}, 'test::destroy::1';
  $test::destroy::1::destroyed = 0;
  undef $db;
  is $test::destroy::1::destroyed, 1;
} # _destroy_connected

sub _destroy_executed : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge';
  my $db = Dongry::Database->new;
  $db->source (default => {dsn => $dsn, anyevent => 1});
  $db->execute ('show tables');
  $db->{dummy} = bless {}, 'test::destroy::1';
  $test::destroy::1::destroyed = 0;
  undef $db;
  is $test::destroy::1::destroyed, 1;
} # _destroy_executed

sub _destroy_executed_ae : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'hoge';
  my $db = Dongry::Database->new;
  $db->source (default => {dsn => $dsn, anyevent => 1});

  my $cv = AnyEvent->condvar;

  $db->execute ('show tables', undef, cb => sub {
    $cv->send;
  });

  $db->{dummy} = bless {}, 'test::destroy::1';

  $test::destroy::1::destroyed = 0;
  $cv->recv;

  undef $db;
  is $test::destroy::1::destroyed, 1;
} # _destroy_executed_ae

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
