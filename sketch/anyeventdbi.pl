use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use lib file (__FILE__)->dir->parent->subdir ('modules', 'perl-rdb-utils', 'lib')->stringify;

use DBIx::ShowSQL;
use Test::MySQL::CreateDatabase qw(test_dsn);

my $dsn = test_dsn 'hoge';

use AnyEvent::DBI::Carp;
use AnyEvent::DBI::Hashref;
my $cv = AnyEvent->condvar;

=pod

{
  package My::AnyEvent::DBI;
  use base qw(AnyEvent::DBI);

  for my $cmd_name (qw(exec_as_hashref)) {
   eval 'sub ' . $cmd_name . '{
      my $cb = pop;

      #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
      my $i = Carp::short_error_loc() || Carp::long_error_loc();
      my %caller = Carp::caller_info $i;
      warn sprintf "Called at %s line %d\n", $caller{file}, $caller{line};

      splice @_, 1, 0, $cb,

      #(caller($Carp::CarpLevel))[1,2],
      $caller{file}, $caller{line},

      "req_' . $cmd_name . '";
      &AnyEvent::DBI::_req
   }';
 }

#our $DBH;
sub AnyEvent::DBI::req_exec_as_hashref {
  my (undef, $st, @args) = @{+shift};
   my $sth = $AnyEvent::DBI::DBH->prepare_cached ($st, undef, 1)
       or die [$DBI::errstr];

   my $rv = $sth->execute (@args)
      or die [$sth->errstr];

   [1, $sth->{NUM_OF_FIELDS} ? $sth->fetchall_arrayref(+{}) : undef, $rv]
}

}

=cut


my $dbh = AnyEvent::DBI::Carp->new ($dsn, "", "", 

on_connect => sub {
  warn "onconnect...";
},
on_error => sub {

  my ($dbh, $filename, $line, $fatal) = @_;

  warn "onerror <@_>";
  die "ONerror";
});
my $dbh2 = AnyEvent::DBI::Carp->new ($dsn, "", "");

warn "create..>";
$dbh->exec ('create table hoge (id int primary key, hoge blob) engine = innodb', sub { });

warn "insert...";
$dbh->exec ('insert into hoge (id) values (1), (3), (10)', sub { });

$cv->begin;

$cv->begin;
warn "select..";
$dbh->exec ("select * from hoge where id=? or id = 3", 10, sub {
  my ($dbh, $rows, $rv) = @_;

  $#_ or do {
    warn "!!!1 failure: $@";
    $cv->end;
    return;
  };

  use Data::Dumper;
  warn Dumper $rows;
  
  $cv->end;
});

$cv->begin;
warn "select..2";
$dbh->exec_as_hashref ("select * from hoge where id= 3 or id = 10", sub {
  my ($dbh, $rows, $rv) = @_;

  warn "...";

  $#_ or do {
    warn "?2 failure: $@";
    $cv->end;
    return;
  };

  use Data::Dumper;
  warn Dumper $rows;

  
  $cv->end;
});



$cv->begin;
warn "select..3";

my $abc = sub {
  local $Carp::CarpLevel = $Carp::CarpLevel + 1;
$dbh->exec_as_hashref ("select * from hoge where fuga = 12", sub {
  my ($dbh, $rows, $rv) = @_;

  warn "3 result ...";
  warn "@_";

  $#_ or do {
    warn "3 failure: $@";
    $cv->end;
    return;
  };
  
  $cv->end;
});
};


$abc->();


$cv->begin;
warn "select..4";
$dbh->exec_as_hashref ("select * from hoge where id= 3", sub {
  my ($dbh, $rows, $rv) = @_;
  $#_ or do {
    warn "?4 failure: $@";
    $cv->end;
    return;
  };

  warn "4 done";
  $cv->end;
});


$cv->end;

$cv->recv;
warn "Ended";

__END__

$cv = AnyEvent->condvar;

$cv->begin (sub { warn 100; $_[0]->send });

$cv->begin;
$dbh->exec ('select * from hoge where id = 1', sub { 
  $cv->end;
});

$cv->begin;
$dbh->exec ('select * from hoge where id = 3', sub { 
  $cv->end;
});

$cv->end;

$cv->begin;

$cv->begin;
warn "begin...";
$dbh->begin_work (sub {

$cv->begin;
warn "begin2...";
$dbh2->begin_work (sub { });

});
warn "insert...";
$dbh->exec ('insert into hoge (id) values (6)', sub { 

warn "work2...";
$dbh2->exec ('select * from hoge where id = 6 for update', sub { });

warn "workg 1.2";
$dbh->exec ('select * from hoge where id = 6', sub {
  warn "work 1.2 done";
});

warn "commit2";
$dbh2->commit (sub { 

warn "commit";
$dbh->commit (sub { $cv->end });

$cv->end });

});



$cv->end;

$cv->recv;
