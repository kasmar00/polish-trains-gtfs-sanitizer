# Polish Trains GTFS Sanitizer

This reposotiroy will _eventually_ contain code to sanitize all train related gtfs feeds in Poland.

Currently it contains:
- a script `sync.sh` to downlowad and unpack all feeds
- a list of issues with the feeds

## Feeds:

1. PKP Intercity
    - source: mkuran
        - url: https://mkuran.pl/gtfs/pkpic.zip
        - issues:
            - platforms as an extra field (non standard)
            - route short names sometimes wrong (EN, MP, IC+)
    - source: kasmar00
        - url: http://gtfs.kasznia.net/static/pkp-ic.zip
        - notes: same as mkuran, but with platforms
1. Arriva
    - url: https://files.girlc.at/gtfs/arriva.zip
    - source: girlc
    - issues: none, it's perfect
        - no platforms
1. Koleje Dolnośląskie
    - url: https://gtfs.i.kiedyprzyjedzie.pl/kd/google_transit.zip
    - source: official
    - issues:
        - has platforms, but no tracks, generic locations
        - has divided routes (D1, D2, etc) but no regio-express distinction
    - notes:
        - has some long distance trains and international trains, under normal routes (ex. berlin - wrocław is marked as D10)...
1. Koleje Mazowieckie
    - url: https://mkuran.pl/gtfs/kolejemazowieckie.zip
    - source: mkuran
    - issues:
        - no platforms
        - has split routes (R9, RE1, etc)
    - has shapes
1. Koleje Małopolskie
    - url: https://kolejemalopolskie.com.pl/rozklady_jazdy/kml-ska-gtfs.zip
    - source: official
    - issues:
        - stop platform as stop_headsign
        - capitalized stop names (as in ALL CAPS)
    - has shapes
1. Koleje Śląskie
    - url: https://koleje-ks.pl/gtfs/2024-2025.zip
    - source: official
    - issues:
        - no platforms
    - has shapes
1. Koleje Wielkopolskie
    - url: http://ws.kolejewlkp.pl:83/gtfs_kw.zip
    - source: official
    - issues:
        - PKM routes as an extra (non standard) field
        - vehicle type in trip short name
        - routes.txt misses agency_id collumn
        - some trips have two PKMs (ex. `52`) - could either be split in Poznań Główny (and joined with a block) or saved as the destination PKM (ex. `2`). This repository does the latter.
        - BUS (ZKA) is not separated
        - traktion type is in trip_short_name
1. Łódzka Kolej Aglomeracyjna
    - url: https://kolej-lka.pl/pliki/pn0e6eg45qcl4hd5/gtfs-2024-2025/zip/
    - source: official
    - issues:
        - bus routes have no numbering
    - notes:
        - route_id is a concat of fisrt and last stop
        - for buses stop ids encode route number on second and third digit, written backwards (ex: 1100028 -> 1, 1900032 -> 9, 13100033 -> 13)
        - nr_lini from live is mapped to ~trip_short_name
1. Polregio
    - source: official
        - url: https://transfer.polregio.pl/public/file/2xpjhbomseoindotgttcsg/GTFS.zip
        - issues:
            - has platforms(roman)+tracks, but with generic locations
            - each train is it's own route (1:1 between routes and trips)
            - no trip names (ex. `IR Lutynia`, `S3` etc.)
            - some stops are in really weird locations: Łódź Dąbrowa, Alt Rosenthal
            - weird stop names: Angermünde => Angermuende, Bobowa Miasto => Bobowa-Miasto, Frankfurt (Oder)=>Frankfurt/Oder
            - has bikes and wheelchair availability
        - notes:
            - route_short_name: R - Regio, RP - regio fast, IR - interregio
    - source: mkuran
        - url: https://mkuran.pl/gtfs/polregio.zip
        - issues:
            - has platforms and track as non standard column in stop_times
            - no RP 
            - has trip names (ex. Lutynia) but also line names (PKM4, S3, Podkarpacka Kolej Aglomeracyjna), no Pomorska KM
1. Regio Jet
    - url: https://gtfs.kasznia.net/static/regio-jet.zip
    - source: kasmar00
    - issues: None, its ideal :P
1. SKM Warszawa
    - url: https://cdn.zbiorkom.live/gtfs/pkp-skmw.zip
    - source: zbiorkom
    - issues: None, its perfect
        - wrong agency (should be SKM.sa)
    - has shapes
1. SKM Trójmiasto
    - url: https://www.skm.pkp.pl/gtfs-mi-kpd.zip
    - source: official
    - issues:
        - capitalized (as ALL CAPS) headsigns and some stops
        - no platforms
    - notes:
        - trip short name is actuall train_id
        - 1:1:1 between routes and trips and shapes
    - has shapes
1. WKD
    - url: https://mkuran.pl/gtfs/wkd.zip
    - source: mkuran
    - has shapes
    - has fares

