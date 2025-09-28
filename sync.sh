cd feeds-zip

files=(
    "pkp-ar.zip"
    "pkp-kd.zip"
    "pkp-kml.zip"
    "pkp-kw.zip"
    "pkp-pr.zip"
    "pkp-skmw.zip"
    "pkp-km.zip"
    "pkp-ks.zip"
    "pkp-lka.zip"
    "pkp-skmt.zip"
    "pkp-rj.zip"
    )

for i in "${files[@]}"
do
    wget "https://cdn.zbiorkom.live/gtfs/$i" -O $i
done

wget https://mkuran.pl/gtfs/wkd.zip -O wkd.zip
wget https://transfer.polregio.pl/public/file/2xpjhbomseoindotgttcsg/GTFS.zip --no-check-certificate -O polregio-official.zip

folders=(
    "pkp-ar"
    "pkp-kd"
    "pkp-kml"
    "pkp-kw"
    "pkp-pr"
    "pkp-skmw"
    "pkp-km"
    "pkp-ks"
    "pkp-lka"
    "pkp-skmt"
    "pkp-rj"
    "polregio-official"
    "wkd"
)

for i in "${folders[@]}"
do
    unzip -o "$i.zip" -d $i
done