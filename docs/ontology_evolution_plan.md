# Ontology Evolution Plan: Enabling AI-Assisted Troubleshooting

## Goal
Build an ontology that lets an AI answer questions like:
- "The line keeps stopping" → What does that mean?
- "Why is Motor01 not running?"
- "What could cause Valve01 timeout faults?"
- "What's blocking the palletizer from cycling?"
- "Is this alarm normal during startup?"

---

## The Translation Problem

Operators, SCADA, and PLC all describe the same reality differently:

```
┌─────────────────────────────────────────────────────────────────────┐
│  OPERATOR OBSERVES                                                  │
│  "The pusher isn't working" / "Line keeps stopping" / "It's stuck" │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SCADA SHOWS                                                        │
│  Valve01 faceplate: Status = FAULTED, Alarm = "Extend Timeout"     │
│  Dashboard: Line Status = BLOCKED, Downtime counting               │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PLC STATE                                                          │
│  Valve01.bOutFaultTimeout = TRUE                                   │
│  Valve01.bInSignalWork = FALSE (never saw extended position)       │
│  Valve01.HMI_Status = 6 (fault code)                               │
│  Air_Pressure_OK = TRUE (so not air supply)                        │
└─────────────────────────────────────────────────────────────────────┘
```

**The ontology needs to bridge all three levels.**

---

## Proposed: Operator-to-PLC Translation Layer

### Operator Language Dictionary

Map natural language observations to SCADA/PLC concepts:

```yaml
operator_language:
  "line keeps stopping":
    means: "Production sequence interrupted repeatedly"
    scada_indicators:
      - "Line Status != RUNNING"
      - "Downtime alarm active"
      - "Cycle count not incrementing"
    plc_signals:
      - "Sequence state machine not in RUN step"
      - "Any equipment fault bit = TRUE"
      - "Interlock chain broken"
    ask_operator:
      - "Does it stop at the same place each time?"
      - "Any alarms on the HMI?"
      - "How long does it run before stopping?"

  "it's stuck":
    means: "Equipment not moving when expected"
    likely_equipment: ["valves", "cylinders", "conveyors", "motors"]
    scada_indicators:
      - "Equipment faceplate shows fault or timeout"
      - "Position indicator not matching command"
    plc_signals:
      - "Command active but feedback not received"
      - "Timeout timer expired"
      - "Motion in progress bit stuck TRUE"
    ask_operator:
      - "Which station or equipment?"
      - "Extended or retracted position?"
      - "Can you hear it trying to move?"

  "pusher isn't working":
    maps_to: "Valve/cylinder actuator fault"
    scada_element: "ValveSolenoidControl faceplate"
    scada_indicators:
      - "Status != WORK or HOME position"
      - "Fault icon visible"
      - "Alarm banner shows valve name"
    plc_signals:
      - "Valve_Solenoid.bOutFaultTimeout"
      - "Valve_Solenoid.bInSignalWork == FALSE after command"
      - "Valve_Solenoid.bInSignalHome == FALSE after retract"
    common_causes:
      - "Air pressure low"
      - "Solenoid valve stuck"
      - "Position sensor failed/misaligned"
      - "Mechanical jam"

  "motor won't run":
    maps_to: "Motor start failure"
    scada_element: "MotorReversingControl faceplate"
    scada_indicators:
      - "Status = STOPPED when should be RUNNING"
      - "Fault indicator lit"
      - "Run feedback not matching command"
    plc_signals:
      - "Motor_Reversing.bInInterlock == FALSE"
      - "Motor_Reversing.bOutFaultOverload == TRUE"
      - "Motor_Reversing.bInEStop == FALSE"
      - "Motor_Reversing.bInSignalForward == FALSE despite command"
    common_causes:
      - "Interlock not satisfied"
      - "E-Stop active"
      - "Overload tripped"
      - "Contactor failure"

  "wrong count" / "count is off":
    means: "Production count doesn't match actual"
    scada_element: "Dashboard part counters"
    plc_signals:
      - "IO_DigitalInput sensor for counting"
      - "Debounce settings (double-counting?)"
      - "Sensor bOutOn toggling correctly?"
    common_causes:
      - "Sensor sensitivity (double-trips)"
      - "Debounce too short"
      - "Product spacing irregular"
      - "Sensor misaligned"

  "running slow" / "cycle time is bad":
    means: "Production rate below expected"
    scada_indicators:
      - "Parts per minute below target"
      - "Cycle time trending up"
    plc_signals:
      - "Timeout values being approached"
      - "Dwell timers"
      - "Sequence step times"
    common_causes:
      - "Air pressure marginal"
      - "Mechanical wear"
      - "Sensor response slow"
      - "Upstream/downstream bottleneck"

  "alarm keeps coming back":
    means: "Recurring fault condition"
    scada_indicators:
      - "Same alarm in history repeating"
      - "Alarm count incrementing"
    plc_signals:
      - "Fault condition clearing then re-triggering"
      - "Intermittent sensor signal"
    common_causes:
      - "Root cause not addressed"
      - "Sensor marginal/failing"
      - "Mechanical issue getting worse"
      - "Electrical noise"
```

### Bidirectional Mapping

The ontology should work both directions:

**Operator → PLC (Troubleshooting)**
```
"The pusher is stuck extended"
    → SCADA: Valve01 faceplate, check status
    → PLC: Valve01.bInSignalWork should be TRUE if actually extended
         Valve01.bOutRetract should be TRUE if trying to retract
         → If both true but not moving: mechanical jam or air
         → If bOutRetract FALSE: command not issued, check sequence
```

**PLC → Operator (Explaining)**
```
Valve01.bOutFaultTimeout == TRUE
    → SCADA: Valve01 shows "Timeout Fault" alarm
    → Operator: "The pusher tried to move but didn't reach
                 position in time. Check for mechanical
                 obstruction or air supply."
```

---

## Current State

### What We Have
- **AOI semantics**: Tags, relationships, interlocks extracted
- **PLC-to-SCADA mappings**: Know which HMI elements connect to which logic
- **Control patterns**: Debounce, mutual exclusion, handshakes identified

### What's Missing for Troubleshooting

| Gap | Why It Matters for Troubleshooting |
|-----|-----------------------------------|
| **Functional Intent** | AI can't explain *why* an interlock exists or what failure it prevents |
| **Causal Chains** | No "if X then Y" reasoning paths for fault diagnosis |
| **Symptom-to-Cause Mapping** | Can't link "motor not running" to possible root causes |
| **Normal vs Abnormal States** | AI doesn't know what's expected in each mode |
| **Temporal Context** | No understanding of "this is normal during startup" |

---

## Proposed Additions for Troubleshooting

### 1. Fault Trees / Causal Chains

For each equipment type, define what prevents operation:

```yaml
Motor_Reversing:
  symptom: "Motor not running"
  possible_causes:
    - condition: "bInEStop == FALSE"
      explanation: "E-Stop is active"
      check: "Verify E-Stop buttons are released and reset"

    - condition: "bInInterlock == FALSE"
      explanation: "Safety interlock chain is open"
      check: "Check upstream interlocks, guards, permits"

    - condition: "bInCommandForward == FALSE AND bInCommandReverse == FALSE"
      explanation: "No run command issued"
      check: "Verify auto sequence is requesting motion, or manual button pressed"

    - condition: "bOutFaultOverload == TRUE"
      explanation: "Motor overload tripped"
      check: "Check motor starter, thermal overload, reset if safe"

    - condition: "nMode != 1 (Auto)"
      explanation: "Not in Auto mode"
      check: "Equipment may be in Manual or taken out of service"
```

### 2. Intent Annotations (Why Things Exist)

```yaml
Motor_Reversing.interlocks:
  forward_reverse_mutex:
    what: "Cannot command forward and reverse simultaneously"
    why: "Prevents gearbox destruction and motor burnout"
    symptom_if_violated: "Would cause immediate mechanical damage"

  interlock_chain:
    what: "Motion blocked when bInInterlock is FALSE"
    why: "Upstream equipment not ready, or safety condition not met"
    symptom_if_open: "Motor won't respond to commands"
    typical_causes: ["Guard door open", "Upstream equipment faulted", "E-stop chain broken"]

  timeout_fault:
    what: "Fault if feedback not received within tInTimeout"
    why: "Detects mechanical jam, broken coupling, or sensor failure"
    symptom: "Motor commanded but no motion detected"
    typical_causes: ["Mechanical obstruction", "Feedback sensor failed", "Coupling broken"]
```

### 3. Expected States by Context

```yaml
Motor01:
  during_startup:
    expected: "STOPPED, no faults"
    normal_alarms: ["Communication initializing"]

  during_production:
    expected: "Running forward or at position"
    abnormal_if: "Stopped for >30s without command"

  during_shutdown:
    expected: "STOPPED"
    normal_alarms: ["Sequence complete"]

  during_changeover:
    expected: "May be in Manual mode"
    normal: "Operator controlling directly"
```

### 4. Symptom-to-Diagnosis Lookup

```yaml
symptoms:
  "Motor running but no product moving":
    likely_causes:
      - "Mechanical coupling failure"
      - "Conveyor belt slipping"
      - "Product jammed downstream"
    checks:
      - "Verify encoder/tach feedback matches command"
      - "Visual inspection of mechanical drive"
      - "Check downstream sensors"

  "Valve timeout fault":
    likely_causes:
      - "Air pressure low"
      - "Valve solenoid failed"
      - "Position sensor misaligned"
      - "Mechanical obstruction"
    checks:
      - "Check air pressure gauge"
      - "Listen for solenoid click"
      - "Verify sensor LEDs"
      - "Manual actuation test"

  "Intermittent sensor faults":
    likely_causes:
      - "Loose wiring connection"
      - "Electrical noise"
      - "Sensor sensitivity too high"
      - "Target inconsistent"
    checks:
      - "Wiggle test on connections"
      - "Check for VFD/motor noise sources"
      - "Adjust sensor if possible"
```

### 5. Cross-Reference: What Affects What

```yaml
Motor01:
  affects:
    - "Downstream conveyor position sensors"
    - "Part count accuracy"
    - "Cycle time"
  affected_by:
    - "E-Stop chain"
    - "Upstream equipment ready signal"
    - "Air pressure (if pneumatic brake)"

Valve01:
  affects:
    - "Product pusher position"
    - "Cycle cannot advance until valve confirmed"
  affected_by:
    - "Air supply pressure"
    - "Motor01 position (may be interlocked)"
```

---

## Implementation Approach

### Step 1: Enrich Existing AOI Ontologies
Add to each AOI analysis:
- `fault_tree`: List of conditions that prevent operation
- `intents`: Why each interlock/feature exists
- `typical_failures`: Common failure modes and symptoms

### Step 2: Add Symptom Database
Create lookup from observable symptoms to diagnostic paths:
- Symptom description
- Likely causes (ranked)
- Diagnostic checks
- Related equipment

### Step 3: Add Relationship Graph for "Affects/Affected By"
Enable questions like:
- "What else might be affected if Motor01 is down?"
- "What could be causing this downstream sensor to fail?"

### Step 4: Add Operational Context
- Expected states per mode (startup, run, shutdown, maintenance)
- Normal vs abnormal alarm patterns
- Time-based expectations ("normal to see X for first 30 seconds")

---

## Example AI Troubleshooting Flow

**User**: "Motor01 keeps faulting on timeout"

**AI with enhanced ontology**:
1. Looks up `Motor01` → `Motor_Reversing` AOI
2. Finds `timeout_fault` intent: "Detects mechanical jam or sensor failure"
3. Retrieves `typical_causes`: ["Mechanical obstruction", "Feedback sensor failed", "Coupling broken"]
4. Gets `diagnostic_checks`:
   - "Verify bInSignalForward/Reverse feedback is toggling when motor runs"
   - "Check mechanical drive train for obstruction"
   - "Inspect feedback sensor alignment and function"
5. Checks `affected_by`: "Air pressure (if pneumatic brake)" → "Verify air supply"

**AI Response**:
> Motor01 timeout fault means the motor was commanded but position feedback wasn't received within the timeout period. Common causes:
> 1. **Mechanical obstruction** - Check for jammed product or debris
> 2. **Feedback sensor failure** - Verify the proximity sensor LED is toggling when motor shaft rotates
> 3. **Coupling failure** - Motor may be spinning but not driving the load
> 4. **Pneumatic brake not releasing** - If equipped, check air pressure
>
> Start by watching the feedback sensor while jogging the motor manually.

---

## Open Questions

1. How much of this can be auto-inferred from existing AOI analysis vs manually annotated?
2. Should symptom database be per-AOI or global?
3. How to handle site-specific quirks ("Motor01 always takes 2 tries on cold start")?
4. Integration with live data - can we query current tag values during troubleshooting?

---

## Success Criteria

| Question Type | Current | Target |
|--------------|---------|--------|
| "What is Motor01?" | ✓ Can answer | ✓ |
| "Why won't Motor01 run?" | ✗ | ✓ Fault tree traversal |
| "What does this interlock protect against?" | ✗ | ✓ Intent lookup |
| "Is this alarm normal during startup?" | ✗ | ✓ Context-aware |
| "What else might be affected?" | ✗ | ✓ Dependency graph |

