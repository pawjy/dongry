package Dongry::Type::Text;
use strict;
use warnings;
use Encode;

$Dongry::Types->{text} = {
  parse => sub {
    return defined $_[0] ? decode 'utf-8', $_[0] : $_[0];
  }, # parse
  serialize => sub {
    return defined $_[0] ? encode 'utf-8', $_[0] : $_[0];
  }, # serialize
}; # text

$Dongry::Types->{text_as_ref} = {
  parse => sub {
    return \(defined $_[0] ? decode 'utf-8', $_[0] : $_[0]);
  }, # parse
  serialize => sub {
    return defined ${$_[0]} ? encode 'utf-8', ${$_[0]} : $_[0];
  }, # serialize
}; # text_as_ref

1;
