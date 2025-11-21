document.addEventListener("DOMContentLoaded", () => {
  const page = document.body.dataset.page;
  if (page === "triage") initTriagePage();
  if (page === "home") initHomePage();
});

function initHomePage() {
  const btn = document.getElementById("demoScenarioBtn");
  if (!btn) return;
  btn.addEventListener("click", () => {
    window.location.href = "triage.html";
  });
}

function initTriagePage() {
  const probSlider = document.getElementById("probSlider");
  const probInput = document.getElementById("probInput");
  const probLabel = document.getElementById("probLabel");
  const calcBtn = document.getElementById("calcBtn");

  const expectedRecoveryEl = document.getElementById("expectedRecovery");
  const estimatedCostEl = document.getElementById("estimatedCost");
  const roiValueEl = document.getElementById("roiValue");
  const riskPill = document.getElementById("riskPill");
  const bandSummary = document.getElementById("bandSummary");
  const priorityTag = document.getElementById("priorityTag");
  const actionAdvice = document.getElementById("actionAdvice");
  const reasonList = document.getElementById("reasonList");

  const amountInput = document.getElementById("amount");
  const costInput = document.getElementById("cost");
  const liabilitySelect = document.getElementById("liability");
  const timingSelect = document.getElementById("timing");

  const chips = document.querySelectorAll(".chip");

  function syncFromSlider() {
    const probPercent = Number(probSlider.value);
    const prob = clamp(probPercent / 100, 0, 1);
    probInput.value = prob.toFixed(2);
    probLabel.textContent = `${probPercent}%`;
  }

  function syncFromInput() {
    let prob = Number(probInput.value);
    if (Number.isNaN(prob)) prob = 0;
    prob = clamp(prob, 0, 1);
    const probPercent = Math.round(prob * 100);
    probSlider.value = probPercent;
    probInput.value = prob.toFixed(2);
    probLabel.textContent = `${probPercent}%`;
  }

  probSlider.addEventListener("input", () => {
    syncFromSlider();
    recalc();
  });

  probInput.addEventListener("change", () => {
    syncFromInput();
    recalc();
  });

  [amountInput, costInput, liabilitySelect, timingSelect].forEach(el => {
    el.addEventListener("change", recalc);
  });

  calcBtn.addEventListener("click", recalc);

  chips.forEach(chip => {
    chip.addEventListener("click", () => {
      const scenario = chip.dataset.scenario;
      applyScenario(scenario);
      recalc();
    });
  });

  function applyScenario(type) {
    if (type === "high") {
      probSlider.value = 88;
      syncFromSlider();
      amountInput.value = 90000;
      costInput.value = 12000;
      liabilitySelect.value = "clear";
      timingSelect.value = "urgent";
    } else if (type === "borderline") {
      probSlider.value = 48;
      syncFromSlider();
      amountInput.value = 45000;
      costInput.value = 15000;
      liabilitySelect.value = "medium";
      timingSelect.value = "normal";
    } else if (type === "low") {
      probSlider.value = 18;
      syncFromSlider();
      amountInput.value = 12000;
      costInput.value = 7000;
      liabilitySelect.value = "unclear";
      timingSelect.value = "loose";
    }
  }

  function recalc() {
    const prob = Number(probInput.value);
    const amount = Number(amountInput.value);
    const cost = Number(costInput.value);
    const liability = liabilitySelect.value;
    const timing = timingSelect.value;

    if (Number.isNaN(prob) || Number.isNaN(amount) || Number.isNaN(cost)) return;

    const bandInfo = getBand(prob);
    const expected = prob * amount;
    const { roi, priority } = getPriority(expected, cost, bandInfo.band, liability, timing);

    expectedRecoveryEl.textContent = formatMoney(expected);
    estimatedCostEl.textContent = formatMoney(cost);
    roiValueEl.textContent = roi === Infinity ? "∞" : `${roi.toFixed(2)}×`;

    riskPill.textContent = `Band ${bandInfo.band} · ${bandInfo.label}`;
    riskPill.className = `pill ${bandInfo.pillClass}`;

    bandSummary.textContent = bandInfo.summary;

    priorityTag.textContent = priority.label;
    priorityTag.className = `priority-tag ${priority.className}`;

    actionAdvice.textContent = priority.actionText;

    reasonList.innerHTML = "";
    bandInfo.reasons(expected, cost, liability, timing).forEach(reason => {
      const li = document.createElement("li");
      li.textContent = reason;
      reasonList.appendChild(li);
    });
  }

  syncFromSlider();
  recalc();
}

function clamp(v, min, max) {
  return Math.min(max, Math.max(min, v));
}

function formatMoney(v) {
  if (!Number.isFinite(v)) return "-";
  return v.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function getBand(prob) {
  let band, label, pillClass, summary;
  if (prob >= 0.8) {
    band = "A";
    label = "Top priority";
    pillClass = "pill-A";
    summary = "Very high success likelihood. Fast-track subrogation with full resources.";
  } else if (prob >= 0.6) {
    band = "B";
    label = "Focus";
    pillClass = "pill-B";
    summary = "Good success likelihood. Recommended to pursue with strong follow-up.";
  } else if (prob >= 0.4) {
    band = "C";
    label = "Contested";
    pillClass = "pill-C";
    summary = "Moderate success likelihood. Pursue selectively based on ROI and strategy.";
  } else if (prob >= 0.2) {
    band = "D";
    label = "Low chance";
    pillClass = "pill-D";
    summary = "Low success likelihood. Only pursue in special situations.";
  } else {
    band = "E";
    label = "Do not pursue";
    pillClass = "pill-E";
    summary = "Very low success likelihood. Generally not recommended for subrogation.";
  }

  function reasons(expected, cost, liability, timing) {
    const list = [];
    list.push(`Model probability is ${prob.toFixed(2)}, placing the claim in band ${band}.`);
    if (expected > cost) {
      list.push("Expected recovery is higher than estimated pursuit cost.");
    } else {
      list.push("Expected recovery does not clearly exceed estimated pursuit cost.");
    }
    if (liability === "clear") {
      list.push("Liability is assessed as clear against the counterparty.");
    } else if (liability === "medium") {
      list.push("Liability is mixed, which may reduce negotiating leverage.");
    } else {
      list.push("Liability is unclear, creating additional litigation risk.");
    }
    if (timing === "urgent") {
      list.push("Limitation is urgent, so delays increase the risk of losing the opportunity.");
    } else if (timing === "normal") {
      list.push("Limitation is standard with normal time pressure.");
    } else {
      list.push("Limitation is comfortable, allowing more flexibility in triage.");
    }
    return list;
  }

  return { band, label, pillClass, summary, reasons };
}

function getPriority(expected, cost, band, liability, timing) {
  const roi = cost > 0 ? expected / cost : Infinity;

  let score = 0;

  if (band === "A") score += 3;
  else if (band === "B") score += 2;
  else if (band === "C") score += 1;

  if (roi >= 2) score += 2;
  else if (roi >= 1) score += 1;

  if (liability === "clear") score += 1;
  if (timing === "urgent") score += 1;

  let priority;
  if (score >= 5) {
    priority = {
      label: "High",
      className: "priority-high",
      actionText:
        "Immediately open a subrogation file. Assign to an experienced handler, start negotiations, and prepare litigation as a back-up."
    };
  } else if (score >= 3) {
    priority = {
      label: "Medium",
      className: "priority-mid",
      actionText:
        "Pursue subrogation through standard workflow. Monitor progress and escalate if cooperation breaks down or new evidence emerges."
    };
  } else {
    priority = {
      label: "Low",
      className: "priority-low",
      actionText:
        "Limit effort to light-touch actions (demand letters, simple negotiation). Only escalate in exceptional circumstances."
    };
  }

  return { roi, priority };
}
