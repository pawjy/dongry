package Test::Dongry;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->parent->parent->subdir ('lib')->stringify;
use lib glob file (__FILE__)->dir->parent->parent->parent->subdir ('modules', '*', 'lib')->stringify;
use lib glob file (__FILE__)->dir->parent->parent->parent->subdir ('t_deps', 'modules', '*', 'lib')->stringify;
use lib file (__FILE__)->dir->parent->parent->subdir ('lib')->stringify;

use Exporter::Lite;
our @EXPORT;

use Test::MoreMore;
push @EXPORT, @Test::MoreMore::EXPORT;

use Test::MySQL::CreateDatabase;
Test::MySQL::CreateDatabase->import (@Test::MySQL::CreateDatabase::EXPORT_OK);
push @EXPORT, @Test::MySQL::CreateDatabase::EXPORT_OK;

require DBIx::ShowSQL;

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
