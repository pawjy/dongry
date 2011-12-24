package Test::Dongry;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->parent->parent->subdir ('lib')->stringify;
use lib glob file (__FILE__)->dir->parent->parent->parent->subdir ('modules', '*', 'lib')->stringify;

use Exporter::Lite;
our @EXPORT;

use Test::MoreMore;
push @EXPORT, @Test::MoreMore::EXPORT;

use Test::MySQL::CreateDatabase;
Test::MySQL::CreateDatabase->import (@Test::MySQL::CreateDatabase::EXPORT_OK);
push @EXPORT, @Test::MySQL::CreateDatabase::EXPORT_OK;

require DBIx::ShowSQL;

1;
