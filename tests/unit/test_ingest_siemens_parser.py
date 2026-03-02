from pathlib import Path

from siemens_parser import SiemensSTParser


SAMPLE_ST = """
NAMESPACE Plant.Process

TYPE MotorData : STRUCT
    Speed : REAL;
END_STRUCT
END_TYPE

CLASS MotorFB
VAR_INPUT
    StartCmd : BOOL; // start command
END_VAR
VAR_OUTPUT
    Running : BOOL;
END_VAR
METHOD PUBLIC Execute : BOOL
VAR
    tempVar : INT := 1;
END_VAR
Running := StartCmd;
END_METHOD
END_CLASS

PROGRAM MainProgram
VAR
    Counter : INT := 0;
END_VAR
Counter := Counter + 1;
END_PROGRAM

CONFIGURATION Config1
TASK MainTask(INTERVAL := T#100MS, PRIORITY := 1);
PROGRAM PLC_PRG WITH MainTask: MainProgram;
END_CONFIGURATION

END_NAMESPACE
"""


def test_parse_structured_text_blocks(tmp_path):
    st_path = Path(tmp_path) / "sample.st"
    st_path.write_text(SAMPLE_ST, encoding="utf-8")

    parser = SiemensSTParser()
    blocks = parser.parse_file(str(st_path))
    assert len(blocks) >= 4

    by_name = {b.name: b for b in blocks}
    assert "MotorData" in by_name
    assert by_name["MotorData"].type == "UDT"
    assert by_name["MotorData"].local_tags[0].name == "Speed"

    assert "MotorFB" in by_name
    fb = by_name["MotorFB"]
    assert fb.type == "FB"
    assert any(t.name == "StartCmd" and t.direction == "INPUT" for t in fb.input_tags)
    assert any(t.name == "Running" and t.direction == "OUTPUT" for t in fb.output_tags)
    assert any(r["name"] == "Execute" for r in fb.routines)

    assert "MainProgram" in by_name
    assert by_name["MainProgram"].type == "PROGRAM"
    assert "Counter := Counter + 1" in by_name["MainProgram"].raw_implementation

    assert "Config1" in by_name
    assert by_name["Config1"].type == "CONFIGURATION"
    assert "MainTask" in by_name["Config1"].description

