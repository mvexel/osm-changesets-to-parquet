# OSM Changeset metadata to Parquet

This little tool converts the OSM Changeset metadata dump (.osm.bz2) to a parquet file you can query using DuckDB and friends.

Converting to geoparquet is an option - see examples below.

## Building

```bash
cargo build --release
```

The compiled binary will be at `target/release/changesets-to-parquet`.

Precompiled binaries are also available (look to your right)

## Usage

First, download a changeset dump file from https://planet.osm.org/.

Then:

```bash
./target/release/changesets-to-parquet \
  --input changesets-latest.osm.bz2 \
  --output changesets.parquet \
```

### Arguments

- `--input` or `-i`: Input OSM changeset XML file (.osm or .osm.bz2)
- `--output` or `-o`: Output Parquet file

## Querying with DuckDB

Assuming you are in the directory where you created `changesets.parquet`:

```sql
D select * from 'changesets-251006.parquet' where description ilike '%maproulette%' limit 10;
100% ▕██████████████████████████████████████▏ (00:00:02.55 elapsed)
┌──────────┬─────────────────────┬─────────────────────┬─────────┬─────────┬────────┬─────────────┬──────────────┬─────────────┬──────────────┬─────────────┬────────────────┬───────────────────────────────────────────────────────────────┐
│    id    │     created_at      │      closed_at      │  open   │  user   │  uid   │   min_lat   │   min_lon    │   max_lat   │   max_lon    │ num_changes │ comments_count │                          description                          │
│  int64   │      timestamp      │      timestamp      │ boolean │ varchar │ int64  │   double    │    double    │   double    │    double    │   uint32    │     uint32     │                            varchar                            │
├──────────┼─────────────────────┼─────────────────────┼─────────┼─────────┼────────┼─────────────┼──────────────┼─────────────┼──────────────┼─────────────┼────────────────┼───────────────────────────────────────────────────────────────┤
│ 13499702 │ 2012-10-14 22:45:55 │ 2012-10-14 22:46:32 │ false   │ ToeBee  │ 262151 │  41.1631494 │  -84.2320873 │  41.2577204 │  -84.1799959 │         536 │              0 │ Un-zorroing ways based on the un-zorro-tron (maproulette.org) │
│ 13499724 │ 2012-10-14 22:49:21 │ 2012-10-14 22:49:23 │ false   │ ToeBee  │ 262151 │  40.0548863 │  123.4190223 │  40.0598343 │  123.4238378 │           2 │              0 │ Un-zorroing ways based on the un-zorro-tron (maproulette.org) │
│ 13499742 │ 2012-10-14 22:51:00 │ 2012-10-14 22:51:04 │ false   │ ToeBee  │ 262151 │   40.147698 │   -85.367252 │  40.1494832 │  -85.3616716 │           2 │              0 │ Un-zorroing ways based on the un-zorro-tron (maproulette.org) │
│ 13499772 │ 2012-10-14 22:54:13 │ 2012-10-15 01:16:05 │ false   │ ToeBee  │ 262151 │ -36.4214894 │ -147.1862861 │   64.421275 │  174.2153196 │         118 │              0 │ Un-zorroing ways based on the un-zorro-tron (maproulette.org) │
│ 13501436 │ 2012-10-15 05:56:34 │ 2012-10-15 06:56:35 │ false   │ PemFR   │  86806 │  41.0817794 │   23.5160637 │  41.0845142 │   23.5297353 │           2 │              0 │ unzorrotron (maproulette.org)                                 │
│ 13501438 │ 2012-10-15 05:57:04 │ 2012-10-15 06:57:05 │ false   │ PemFR   │  86806 │  48.8192897 │    7.9649917 │  48.8195203 │    7.9655519 │           2 │              0 │ unzorrotron (maproulette.org)                                 │
│ 13501444 │ 2012-10-15 05:58:38 │ 2012-10-15 06:58:40 │ false   │ PemFR   │  86806 │   39.841969 │  -74.9294671 │  39.8433527 │  -74.9278738 │           2 │              0 │ unzorrotron (maproulette.org)                                 │
│ 13501454 │ 2012-10-15 05:59:43 │ 2012-10-15 06:59:44 │ false   │ PemFR   │  86806 │  37.8086851 │ -122.3637135 │  37.8091743 │ -122.3634094 │           3 │              0 │ unzorrotron (maproulette.org)                                 │
│ 13501468 │ 2012-10-15 06:02:39 │ 2012-10-15 07:02:39 │ false   │ PemFR   │  86806 │ -23.3121163 │  -45.9951251 │ -23.3113556 │  -45.9939026 │           4 │              0 │ unzorrotron (maproulette.org)                                 │
│ 13501481 │ 2012-10-15 06:04:30 │ 2012-10-15 07:04:32 │ false   │ PemFR   │  86806 │  35.7610888 │   51.5201875 │   35.778337 │   51.5327929 │          10 │              0 │ unzorrotron (maproulette.org)                                 │
├──────────┴─────────────────────┴─────────────────────┴─────────┴─────────┴────────┴─────────────┴──────────────┴─────────────┴──────────────┴─────────────┴────────────────┴───────────────────────────────────────────────────────────────┤
│ 10 rows                                                                                                                                                                                                                         13 columns │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql
D select count(1) from 'changesets-251006.parquet' where closed_at is not null and date_part('year', closed_at) = 2025 and description ilike '%maproulette%';
100% ▕██████████████████████████████████████▏ (00:00:08.16 elapsed)
┌──────────┐
│ count(1) │
│  int64   │
├──────────┤
│  501709  │
└──────────┘
```

wow that's a lot :)

First 20 mappers who closed a changeset inside a roughly Salt Lake County bounding box:

```sql
D WITH ranked AS (
      SELECT user, closed_at, id,
             ROW_NUMBER() OVER (PARTITION BY user ORDER BY closed_at) AS rn
      FROM read_parquet('changesets-251006.parquet')
      WHERE min_lon >= -112.116
        AND max_lon <= -111.768
        AND min_lat >= 40.453
        AND max_lat <= 40.878
    )
    SELECT user, closed_at AS first_appeared, id AS first_changeset_in_bbox
    FROM ranked
    WHERE rn = 1
    ORDER BY first_appeared limit 20;
100% ▕██████████████████████████████████████▏ (00:00:02.31 elapsed)
┌─────────────────┬─────────────────────┬─────────────────────────┐
│      user       │   first_appeared    │ first_changeset_in_bbox │
│     varchar     │      timestamp      │          int64          │
├─────────────────┼─────────────────────┼─────────────────────────┤
│ EnigmaCurry     │ 2008-01-30 00:35:15 │                  712603 │
│ amm             │ 2008-03-11 01:15:03 │                  306984 │
│ Martin Vidner   │ 2008-03-18 06:07:34 │                  347605 │
│ mvexel          │ 2008-04-12 22:29:39 │                  122714 │
│ PeartDS         │ 2008-08-14 00:12:45 │                  239844 │
│ InVooDoo        │ 2008-09-08 17:15:26 │                  575680 │
│ Rajesh K Ilango │ 2008-09-22 05:53:54 │                  683195 │
│ Val             │ 2008-11-27 06:42:03 │                  799313 │
│ Tom Brown       │ 2008-12-01 01:43:07 │                   24916 │
│ Ted Percival    │ 2008-12-04 02:13:16 │                   98761 │
│ JakeH           │ 2009-01-12 20:10:58 │                  777692 │
│ Scott Lewis     │ 2009-02-20 19:48:09 │                  576053 │
│ acdaks          │ 2009-03-14 20:48:03 │                  810173 │
│ fastfazz        │ 2009-03-21 04:06:57 │                  842125 │
│ treasureofmara  │ 2009-03-21 16:47:56 │                  842186 │
│ esetzer         │ 2009-03-25 03:58:15 │                  855467 │
│ pwhiting        │ 2009-03-31 20:15:47 │                  873451 │
│ TimmyJ          │ 2009-04-01 17:19:04 │                   40944 │
│ butlerm         │ 2009-04-08 09:32:33 │                  319158 │
│ PA94            │ 2009-04-21 21:31:56 │                  888294 │
├─────────────────┴─────────────────────┴─────────────────────────┤
│ 20 rows                                               3 columns │
└─────────────────────────────────────────────────────────────────┘
```

Most prolific mappers:

```sql
D select user, count(1) changeset_count from 'changesets-251006.parquet' where user is not null group by user order by changeset_count desc limit 20;
┌──────────────────────────────────────────────┬─────────────────┐
│                     user                     │ changeset_count │
│                   varchar                    │      int64      │
├──────────────────────────────────────────────┼─────────────────┤
│ TH0MAS8122                                   │          647639 │
│ b-jazz-bot                                   │          384854 │
│ andrewpmk                                    │          374043 │
│ _phiphou_                                    │          354984 │
│ gscholz                                      │          213788 │
│ kmpoppe (@ Mapillary Update)                 │          191950 │
│ Peter Brodersen                              │          191687 │
│ ratrun                                       │          172438 │
│ chachafish                                   │          163036 │
│ eggie                                        │          152294 │
│ FvGordon                                     │          149847 │
│ Đuro Jiří                                    │          146239 │
│ votre_solution_de_visibilite_digitale_locale │          145732 │
│ VictorIE                                     │          140815 │
│ mueschel                                     │          139175 │
│ OSMF Redaction Account                       │          136236 │
│ wheelmap_visitor                             │          135855 │
│ RichRico                                     │          133487 │
│ nikt--                                       │          131354 │
│ Max-Kritic                                   │          129481 │
├──────────────────────────────────────────────┴─────────────────┤
│ 20 rows                                              2 columns │
└────────────────────────────────────────────────────────────────┘
```

You could create a proper geometry column, but for bbox queries you should not see much of an improvement. And the file will be more than twice as big.

```sql
INSTALL spatial;
LOAD spatial;

create table changesets_geom as select *, ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat) as geom from 'changesets-251006.parquet' where min_lat is not null;
100% ▕██████████████████████████████████████▏ (00:00:32.66 elapsed)

COPY (
    SELECT *, ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat) AS geom
    FROM read_parquet('changesets-251006.parquet')
) TO 'changesets_geom.parquet' (FORMAT PARQUET);
100% ▕██████████████████████████████████████▏ (00:02:50.84 elapsed)
```

### Querying

As I am writing this the parquet file should be generating and being uploaded to R2 storage. Try:

```bash
# Count all changesets (DuckDB fetches only metadata, ~5-10MB transfer)
duckdb -c "SELECT COUNT(*) FROM 'https://changesets.osm.lol/latest.parquet'"

# Find recent MapRoulette changesets
duckdb -c "SELECT id, user, created_at, description
           FROM 'https://changesets.osm.lol/latest.parquet'
           WHERE description ILIKE '%maproulette%'
           AND created_at > '2024-01-01'
           LIMIT 10"

# Analyze changesets in a bounding box (only fetches relevant data!)
duckdb -c "SELECT user, COUNT(*) as changeset_count
           FROM 'https://changesets.osm.lol/latest.parquet'
           WHERE min_lon >= -122.5 AND max_lon <= -122.3
           AND min_lat >= 37.7 AND max_lat <= 37.8
           GROUP BY user
           ORDER BY changeset_count DESC
           LIMIT 20"
```
