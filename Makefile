.PHONY: test demo check

test:
	python3 -m unittest discover -s tests -v

demo:
	python3 examples/control_plane_demo.py

check:
	python3 -m compileall -q proctor tests examples scripts runner/checks.py
	bash -n bin/proctor-firewall runner/run.sh runner/mock/mock.sh
	python3 scripts/check_public_release.py
