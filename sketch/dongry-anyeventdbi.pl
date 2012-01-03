use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use lib file (__FILE__)->dir->parent->subdir ('modules', 'perl-ooutils', 'lib')->stringify;
use lib file (__FILE__)->dir->parent->subdir ('modules', 'perl-rdb-utils', 'lib')->stringify;

#use DBIx::ShowSQL;
use Test::MySQL::CreateDatabase qw(test_dsn);

my $dsn = test_dsn 'hoge';

use Dongry::Database;
use AnyEvent;
use Carp;

my $cv = AnyEvent->condvar;

my $db = Dongry::Database->new
    (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1},
                 default => {dsn => $dsn, anyevent => 1}});
$db->onconnect (sub {
                  my ($self, %args) = @_;
                  warn"Oneonncet",  join " ", %args, "\n";
});
$db->onerror (sub {
  my ($self, %args) = @_;
  warn join " ", %args;
});

$db->execute ('create table foo (id int)');
$db->execute ('insert into foo (id) values (2), (4)');

$cv->begin;
#$cv->begin;
#$db->execute ('select test', undef, cb => sub { $cv->end });

$cv->begin;
$db->execute ('select * from foo', undef, cb => sub {
  my $result = shift;
  use Data::Dumper;
  warn $result->row_count;
  warn Dumper $result->all;
  $cv->end;
}, source_name => 'master');
$cv->end;

$cv->recv;
