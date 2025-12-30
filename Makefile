all: out/kw.zip out/kml.zip out/polregio.zip

out/kw.zip:
	python3 -m kw_sanitizer -f
	docker run --rm -v ./out:/work ghcr.io/mobilitydata/gtfs-validator:latest -i /work/kw.zip -o /work/validator-kw

out/kml.zip:
	python3 -m kml_sanitizer -f
	docker run --rm -v ./out:/work ghcr.io/mobilitydata/gtfs-validator:latest -i /work/kml.zip -o /work/validator-kml

out/polregio.zip:
	python3 -m polregio_sanitizer -f
	docker run --rm -v ./out:/work ghcr.io/mobilitydata/gtfs-validator:latest -i /work/polregio.zip -o /work/validator-pr -c PL

.PHONY: out/kw.zip out/kml.zip out/polregio.zip
