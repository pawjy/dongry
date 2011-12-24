use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use lib file (__FILE__)->dir->parent->subdir ('modules', 'perl-rdb-utils', 'lib')->stringify;

use DBIx::ShowSQL;
use Test::MySQL::CreateDatabase qw(test_dsn);
use Dongry::Database;
use Data::Dumper;
use Dongry::Type::DateTime;

my $dsn = test_dsn 'hoge';

$Dongry::Database::Registry->{hoge} = {
  sources => {
    default => {dsn => $dsn},
    master => {dsn => $dsn, writable => 1},
    heavy => {dsn => $dsn},
  },
  onconnect => sub {
    my ($self, %args) = @_;
    #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
    local $Carp::CarpLevel = -1;
    warn Dumper $self->execute
        ('SELECT @@global.time_zone, @@session.time_zone', [],
         source_name => $args{source_name})->first;
    $self->execute ('set time_zone = "+00:00"', [],
                    source_name => $args{source_name},
                    even_if_read_only => 1);
    warn Dumper $self->execute
        ('SELECT @@global.time_zone, @@session.time_zone', [],
         source_name => $args{source_name})->first;
  },
  schema => {
    hoge => {
      table_name => 'hoge',
      primary_keys => ['foo'],
      type => {
        created_on => 'timestamp_as_DateTime',
        created2 => 'timestamp_as_DateTime',
        text => 'text_as_ref',
      },
      default => {
        created2 => sub { require DateTime; return DateTime->now (time_zone => 'UTC') },
        text2 => 'default',
      },
    },
  },
};

my $db = Dongry::Database->load ('hoge');

$db->execute ('show databases')->each (sub {
  my $hashref = $_;
  warn Dumper $hashref;
});

warn Dumper $db->execute ('create table hoge (
  foo bigint unsigned not null,
  created_on timestamp default current_timestamp(),
  value VARCHAR(128),
  text TEXT,
  text2 VARCHAR(123),
  ac bigint unsigned auto_increment,
  created2 timestamp default 0,
  primary key (foo),
  key (ac)
) DEFAULT CHARSET=BINARY, ENGINE=InnoDB');

warn Dumper $db->execute ('show create table hoge')->each (sub {
  warn Dumper $_;
});

warn Dumper $db->execute ('insert into hoge (foo,value) values (123,"abc")');

my $transaction = $db->transaction;

warn Dumper $db->execute ('insert into hoge (foo) values (456),
 (545)');

$db->execute ('select * from hoge')->each (sub {
  my $hashref = $_;
  warn Dumper $hashref;
});

$transaction->commit;

my $transaction2 = $db->transaction;

$db->execute ('insert into hoge (foo) values (789)');
#warn $db->{dbh}->last_insert_id(undef,undef,undef,undef);

$transaction2->rollback;

$db->disconnect;

warn "...";
$db->execute ('select * from hoge')->each (sub {
  my $hashref = $_;
  warn Dumper $hashref;
});

warn "Result:";
$db->execute ('select * from hoge where value is null and foo = ?',
              [1])->each (sub {
  warn Dumper $_;
});

#$db->{dbh}->do('select * from hoge where foo = ?', undef, 1);

my $result = $db->execute ('select * from hoge where foo = 123');
$result->table_name ('hoge');
my $first = $result->first_as_row;
$first->update ({value => 'abc def', value => undef, text => $db->bare_sql_fragment ('"\x{1000}"')});

my $row_orig = $db->table ('hoge')->insert ([{
  foo => 15222,
  value => 'a  eg aa aeee',
  text => my $text = $db->bare_sql_fragment ("\x{2100}\x{3100}"),
}, {
  foo => 3333,
}])->first_as_row;
warn $text;
#warn $row_orig->get ('text');
my $row = $row_orig->reload;

my $created = $row->get ('created_on');
$created->set_time_zone ('Asia/Tokyo');
warn $created;

warn Dumper [$row->get ('text'),
             $row->get_bare ('text')];

$db->delete ('hoge', {
  value => 'a  eg aa aeee',
});

warn Dumper $db->select ('hoge', {
  value => 'a  eg aa aeee',
}, limit => 2, source_name => 'heavy')->all_as_rows->each (sub {
  $_[0]->update ({value => 'xyz'});
});

warn Dumper $db->execute ('select * from hoge')->all;

warn Dumper $db->select ('hoge', ['value is null or value = ?',
                                  value => 'xyz'])->all;

warn $DBIx::ShowSQL::SQLCount;
