import pytest

from app.name_checker.rules import RULES


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("NGC 905", "NGC 905"),
        ("NGC905", "NGC 905"),
        ("NGC00905", "NGC 905"),
        ("NGC  905", "NGC 905"),
        ("ngc 905", "NGC 905"),
        ("NGC", None),
        ("NGC ", None),
        ("M 31", None),
        ("UGC 123", None),
        ("", None),
    ],
)
def test_ngc_rule(input_name: str, expected: str | None) -> None:
    ngc_rule = next(r for r in RULES if r.name == "NGC")
    if expected is None:
        assert ngc_rule.match(input_name) is None
    else:
        assert ngc_rule.match(input_name) == expected


def test_unmatched_name_returns_none_from_all_rules() -> None:
    for rule in RULES:
        assert rule.match("unknown catalog XYZ 123") is None


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("SDSSJ121551.62+573421.6", "SDSS J121551.62+573421.6"),
        ("SDSSJ121552.44+294932.9", "SDSS J121552.44+294932.9"),
        ("SDSSJ121553.17+202452.4", "SDSS J121553.17+202452.4"),
        ("sdssj121551.62+573421.6", "SDSS J121551.62+573421.6"),
        ("SDSSJ121551.62-573421.6", "SDSS J121551.62-573421.6"),
        ("SDSSJ121551.62+57342.6", None),
        ("SDSS J121551.62+573421.6", None),
        ("", None),
    ],
)
def test_sdss_rule(input_name: str, expected: str | None) -> None:
    sdss_rule = next(r for r in RULES if r.name == "SDSS")
    if expected is None:
        assert sdss_rule.match(input_name) is None
    else:
        assert sdss_rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("PGC1191069", "PGC 1191069"),
        ("PGC1119121", "PGC 1119121"),
        ("PGC1425552", "PGC 1425552"),
        ("PGC 1191069", "PGC 1191069"),
        ("PGC001191069", "PGC 1191069"),
        ("pgc1191069", "PGC 1191069"),
        ("PGC", None),
        ("PGC ", None),
        ("UGC 123", None),
        ("", None),
    ],
)
def test_pgc_rule(input_name: str, expected: str | None) -> None:
    pgc_rule = next(r for r in RULES if r.name == "PGC")
    if expected is None:
        assert pgc_rule.match(input_name) is None
    else:
        assert pgc_rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("2MASSJ16295276+3911433", "2MASS J16295276+3911433"),
        ("2massj16295276+3911433", "2MASS J16295276+3911433"),
        ("2MASSJ16295276+391143", None),
        ("2MASS J16295276+3911433", None),
        ("", None),
    ],
)
def test_2mass_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "2MASS")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("2MASXJ21024892-2410520", "2MASX J21024892-2410520"),
        ("2MASXJ09592150-2733330", "2MASX J09592150-2733330"),
        ("2MASXJ10274703+2725368", "2MASX J10274703+2725368"),
        ("2masxj09592150-2733330", "2MASX J09592150-2733330"),
        ("2MASSJ16295276+3911433", None),
        ("", None),
    ],
)
def test_2masx_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "2MASX")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("WINGSJ125634.96-173656.6", "WINGS J125634.96-173656.6"),
        ("WINGSJ232248.36+143927.6", "WINGS J232248.36+143927.6"),
        ("WINGSJ234427.24+091831.8", "WINGS J234427.24+091831.8"),
        ("WINGSJ005617.13-012206.3", "WINGS J005617.13-012206.3"),
        ("WINGSJ132627.17-271414.2", "WINGS J132627.17-271414.2"),
        ("wingsj125634.96-173656.6", "WINGS J125634.96-173656.6"),
        ("WINGS J125634.96-173656.6", None),
        ("", None),
    ],
)
def test_wings_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "WINGS")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("GAMA077963", "GAMA 77963"),
        ("GAMA028694", "GAMA 28694"),
        ("GAMA297074", "GAMA 297074"),
        ("GAMA345748", "GAMA 345748"),
        ("GAMA 77963", "GAMA 77963"),
        ("gama077963", "GAMA 77963"),
        ("GAMA", None),
        ("", None),
    ],
)
def test_gama_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "GAMA")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("MGC56790", "MGC 56790"),
        ("MGC47128", "MGC 47128"),
        ("MGC 56790", "MGC 56790"),
        ("mgc56790", "MGC 56790"),
        ("MGC", None),
        ("", None),
    ],
)
def test_mgc_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "MGC")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("HIP113070", "HIP 113070"),
        ("HIP 113070", "HIP 113070"),
        ("hip113070", "HIP 113070"),
        ("HIP", None),
        ("", None),
    ],
)
def test_hip_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "HIP")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("HD097243", "HD 97243"),
        ("HD044063", "HD 44063"),
        ("HD073216", "HD 73216"),
        ("HD158365", "HD 158365"),
        ("HD114550", "HD 114550"),
        ("HD267907", "HD 267907"),
        ("HD073612", "HD 73612"),
        ("HD 97243", "HD 97243"),
        ("hd097243", "HD 97243"),
        ("HD", None),
        ("", None),
    ],
)
def test_hd_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "HD")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("BD+062136", "BD +062136"),
        ("BD+391991", "BD +391991"),
        ("BD-123456", "BD -123456"),
        ("BD +062136", "BD +062136"),
        ("bd+062136", "BD +062136"),
        ("BD062136", None),
        ("", None),
    ],
)
def test_bd_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "BD")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("And XXVIII", "Andromeda 28"),
        ("Andromeda XXVIII", "Andromeda 28"),
        ("And28", "Andromeda 28"),
        ("ANDXXVIII", "Andromeda 28"),
        ("andxxviii", "Andromeda 28"),
        ("andromeda28", "Andromeda 28"),
        ("And 28", "Andromeda 28"),
        ("Andromeda I", "Andromeda 1"),
        ("Andromeda IV", "Andromeda 4"),
        ("And X", "Andromeda 10"),
        ("Andromeda", None),
        ("And", None),
        ("", None),
    ],
)
def test_andromeda_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "Andromeda")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("ESO104-044", "ESO 104-44"),
        ("ESO381-020", "ESO 381-20"),
        ("ESO 104-044", "ESO 104-44"),
        ("eso104-044", "ESO 104-44"),
        ("ESO104", None),
        ("ESO", None),
        ("", None),
    ],
)
def test_eso_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "ESO")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("HIPASSJ1558-10", "HIPASS J1558-10"),
        ("HIPASS J1348-37", "HIPASS J1348-37"),
        ("hipassj1558-10", "HIPASS J1558-10"),
        ("HIPASSJ1558-1", None),
        ("HIPASS 1348-37", None),
        ("", None),
    ],
)
def test_hipass_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "HIPASS")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("UGC05983", "UGC 5983"),
        ("UGC 5983", "UGC 5983"),
        ("ugc05983", "UGC 5983"),
        ("UGC", None),
        ("", None),
    ],
)
def test_ugc_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "UGC")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("IC1613", "IC 1613"),
        ("IC 1613", "IC 1613"),
        ("ic1613", "IC 1613"),
        ("IC", None),
        ("", None),
    ],
)
def test_ic_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "IC")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("DDO217", "DDO 217"),
        ("DDO226", "DDO 226"),
        ("DDO 217", "DDO 217"),
        ("ddo217", "DDO 217"),
        ("DDO", None),
        ("", None),
    ],
)
def test_ddo_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "DDO")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("AGC724906", "AGC 724906"),
        ("AGC731457", "AGC 731457"),
        ("AGC 724906", "AGC 724906"),
        ("agc724906", "AGC 724906"),
        ("AGC", None),
        ("", None),
    ],
)
def test_agc_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "AGC")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("Dw1311+4051", "Dw 1311+4051"),
        ("Dw1106-0052", "Dw 1106-0052"),
        ("dw1048+1259", "Dw 1048+1259"),
        ("Dw1252-0506", "Dw 1252-0506"),
        ("Dw 1311+4051", "Dw 1311+4051"),
        ("Dw1311+405", None),
        ("Dw1311+40511", None),
        ("Dw", None),
        ("", None),
    ],
)
def test_dw_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "Dw")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("LSBC D640-12", "LSBC D640-12"),
        ("LSBC D565-06", "LSBC D565-6"),
        ("lsbc d640-12", "LSBC D640-12"),
        ("LSBC D640", None),
        ("", None),
    ],
)
def test_lsbc_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "LSBC")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("LV J1235-1104", "LV J1235-1104"),
        ("LV J1052+3628", "LV J1052+3628"),
        ("lv j1235-1104", "LV J1235-1104"),
        ("LV 1235-1104", None),
        ("", None),
    ],
)
def test_lv_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "LV")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("MCG -04-31-038", "MCG -04-31-38"),
        ("MCG -04-31-38", "MCG -04-31-38"),
        ("mcg -04-31-038", "MCG -04-31-38"),
        ("MCG 04-31-038", "MCG 04-31-38"),
        ("MCG", None),
        ("", None),
    ],
)
def test_mcg_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "MCG")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("KDG056", "KDG 56"),
        ("KDG162", "KDG 162"),
        ("KDG 216", "KDG 216"),
        ("kdg056", "KDG 56"),
        ("KDG", None),
        ("", None),
    ],
)
def test_kdg_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "KDG")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("KKSG37", "KKSG 37"),
        ("KKSG34", "KKSG 34"),
        ("kksg37", "KKSG 37"),
        ("KKSG", None),
        ("", None),
    ],
)
def test_kksg_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "KKSG")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("KKH69", "KKH 69"),
        ("KKH 69", "KKH 69"),
        ("kkh69", "KKH 69"),
        ("KK69", None),
        ("KKH", None),
        ("", None),
    ],
)
def test_kkh_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "KKH")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("KKR18", "KKR 18"),
        ("KKR 18", "KKR 18"),
        ("kkr18", "KKR 18"),
        ("KKR", None),
        ("", None),
    ],
)
def test_kkr_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "KKR")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("KK203", "KK 203"),
        ("KK242", "KK 242"),
        ("KK 203", "KK 203"),
        ("kk242", "KK 242"),
        ("KKH69", None),
        ("KKR18", None),
        ("KK", None),
        ("", None),
    ],
)
def test_kk_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "KK")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("BTS132", "BTS 132"),
        ("BTS157", "BTS 157"),
        ("BTS 132", "BTS 132"),
        ("bts132", "BTS 132"),
        ("BTS", None),
        ("", None),
    ],
)
def test_bts_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "BTS")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected


@pytest.mark.parametrize(
    "input_name,expected",
    [
        ("JKB129", "JKB 129"),
        ("JKB 129", "JKB 129"),
        ("jkb129", "JKB 129"),
        ("JKB", None),
        ("", None),
    ],
)
def test_jkb_rule(input_name: str, expected: str | None) -> None:
    rule = next(r for r in RULES if r.name == "JKB")
    if expected is None:
        assert rule.match(input_name) is None
    else:
        assert rule.match(input_name) == expected
