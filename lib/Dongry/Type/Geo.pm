package Dongry::Type::Geo;
use strict;
use warnings;
our $VERSION = '1.0';
use Dongry::Type -Base;
use Dongry::Database;
use Carp;

$Dongry::Types->{geometry} = {
  parse => sub {
    croak "Not supported";
  },
  serialize => sub {
    if (defined $_[0]) {
      if (exists $_[0]->{-lat} and
          exists $_[0]->{-lon}) {
        return Dongry::Database->bare_sql_fragment
            (sprintf "ST_GeomFromText('POINT(%.10f %.10f)')",
                 $_[0]->{-lon}, $_[0]->{-lat});
      } else {
        croak "One or both of |-lat| and |-lon| is not specified";
      }
    } else {
      return undef;
    }
  },
}; # geometry

1;

=head1 LICENSE

Copyright 2011-2022 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
