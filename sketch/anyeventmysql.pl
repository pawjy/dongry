use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use lib file (__FILE__)->dir->parent->subdir ('modules', 'perl-rdb-utils', 'lib')->stringify;
use lib file (__FILE__)->dir->parent->subdir ('t_deps', 'modules', 'AnyEvent-MySQL', 'lib')->stringify;

use DBIx::ShowSQL;
use Test::MySQL::CreateDatabase qw(test_dsn);
use AnyEvent::MySQL;

my $dsn = test_dsn 'hoge';

my $cv = AnyEvent->condvar;

#XXX
$dsn =~ s/dbname/database/;
my $dbh = AnyEvent::MySQL->connect
    ($dsn,
     "root", "", # XXX
     { RaiseError => 1, PrintError => 1, AutoCommit => 1 }, # XXX
     sub {
       
     });

warn "create..>";
$dbh->do ('create table hoge (id int primary key, hoge blob) engine = innodb', sub { });

$cv->begin;

  warn "insert...";
  $dbh->do ('insert into hoge (id) values (1), (3), (10)', sub { 
    $cv->begin;
    warn "select..";
    my $sth = $dbh->prepare ("select * from hoge where id = ?");
    warn "sth";
    $sth->execute (10, sub {
        my $fth = shift;
        
        use Data::Dumper;
        warn Dumper $fth;
      
        $cv->end;
    });

    $cv->end;
  });


$cv->recv;

warn "Ended";

__END__


    $dbh->selectall_hashref ("select * from hoge where id = ?", {}, 10, sub {

      use Data::Dumper;
      warn Dumper \@_;
      
      $cv->end;
    });
