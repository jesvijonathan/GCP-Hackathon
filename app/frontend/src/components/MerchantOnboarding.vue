<script>
import { ref, computed, watch, onMounted } from "vue";
import { useRouter } from "vue-router";

export default {
  name: "ProgressiveForm",
  setup() {
    const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
    const router = useRouter();

    const currentStep = ref(1);
    const steps = ref([
      { title: "Basic Information" },
      { title: "Location & Contact" },
      { title: "Financial & Operational" },
      { title: "Result" },
    ]);

    const defaultRange = () => {
      const now = new Date();
      const start = new Date(now);
      start.setFullYear(now.getFullYear() - 3);
      const end = new Date(now);
      end.setFullYear(now.getFullYear() + 1);
      const toDateStr = (d) =>
        `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(
          2,
          "0"
        )}-${String(d.getDate()).padStart(2, "0")}`;
      return { start: toDateStr(start), end: toDateStr(end) };
    };

    // WLMerchant + range fields
    const formData = ref({
      id: "",
      name: "",
      start_date: "",
      end_date: "",

      merchant_code: "",
      legal_name: "",
      acceptor_name: "",
      acceptor_category_code: "",
      url: "",
      language_code: "",
      time_zone_id: "",

      country_code: "",
      country_sub_division_code: "",
      home_country_code: "",
      region_id: "",
      city: "",
      postal_code: "",
      street: "",
      city_category_code: "",

      business_service_phone_number: "",
      customer_service_phone_number: "",
      additional_contact_information: "",
      description: "",

      currency_code: "",
      tax_id: "",
      trade_register_number: "",
      iban: "",
      domiciliary_bank_number: "",
      cut_off_time: "",
      activation_flag: false,
      activation_time: "",
      activation_start_time: "",
      activation_end_time: "",
    });

    // Preset deep scan mode
    const deepScanMode = ref("none"); // none | redo | replace

    // Suggestion and existing merchants
    const merchantsList = ref([]); // names
    const nameSuggestion = ref("");
    const isSubmitting = ref(false);
    const submitError = ref("");
    const responseData = ref(null);
    const pollTimer = ref(null);
    const pollingIntervalMs = ref(1500);

    // Helpers
    const toHHMMSS = (val) => {
      if (!val) return null;
      const parts = String(val).split(":");
      const hh = (parts[0] || "00").padStart(2, "0");
      const mm = (parts[1] || "00").padStart(2, "0");
      const ss = (parts[2] || "00").padStart(2, "0");
      return `${hh}:${mm}:${ss}`;
    };
    const isoToDatetimeLocal = (iso) => {
      if (!iso) return "";
      try {
        const d = new Date(iso);
        const yyyy = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, "0");
        const dd = String(d.getDate()).padStart(2, "0");
        const hh = String(d.getHours()).padStart(2, "0");
        const mi = String(d.getMinutes()).padStart(2, "0");
        return `${yyyy}-${mm}-${dd}T${hh}:${mi}`;
      } catch {
        return "";
      }
    };
    const hmsToTimeInput = (hms) => {
      if (!hms) return "";
      const parts = String(hms).split(":");
      const hh = (parts[0] || "00").padStart(2, "0");
      const mm = (parts[1] || "00").padStart(2, "0");
      return `${hh}:${mm}`;
    };
    const fmtDateTime = (iso) => {
      if (!iso) return "-";
      try {
        const d = new Date(iso);
        return d.toLocaleString();
      } catch {
        return iso;
      }
    };

    const fetchMerchants = async () => {
      try {
        const resp = await fetch(`${API_BASE}/v1/merchants`, { method: "GET" });
        const data = await resp.json();
        merchantsList.value = Array.isArray(data.merchants)
          ? data.merchants
          : [];
      } catch {
        merchantsList.value = [];
      }
    };

    const updateNameSuggestion = (q) => {
      const query = (q || "").trim().toLowerCase();
      if (!query) {
        nameSuggestion.value = "";
        return;
      }
      const starts = merchantsList.value
        .filter((n) => n && n.toLowerCase().startsWith(query))
        .sort((a, b) => a.length - b.length);
      if (starts.length) {
        nameSuggestion.value = starts[0];
        return;
      }
      const includes = merchantsList.value
        .filter((n) => n && n.toLowerCase().includes(query))
        .sort((a, b) => a.length - b.length);
      nameSuggestion.value = includes[0] || "";
    };

    const isNameUnique = computed(() => {
      const q = (formData.value.name || "").trim().toLowerCase();
      if (!q) return false;
      return !merchantsList.value.some((n) => n.toLowerCase() === q);
    });

    const fillFormFromMerchant = (m) => {
      try {
        formData.value.id = m?.merchant_id || "";
        formData.value.name = m?.merchant_name || formData.value.name;

        formData.value.start_date = m?.start_date || formData.value.start_date;
        formData.value.end_date = m?.end_date || formData.value.end_date;

        formData.value.merchant_code = m?.merchant_code || "";
        formData.value.legal_name = m?.legal_name || "";
        formData.value.acceptor_name = m?.acceptor_name || "";
        formData.value.acceptor_category_code = m?.acceptor_category_code || "";
        formData.value.url = m?.url || "";
        formData.value.language_code = m?.language_code || "";
        formData.value.time_zone_id = m?.time_zone_id || "";

        formData.value.country_code = m?.country_code || "";
        formData.value.country_sub_division_code =
          m?.country_sub_division_code || "";
        formData.value.home_country_code = m?.home_country_code || "";
        formData.value.region_id = m?.region_id || "";
        formData.value.city = m?.city || "";
        formData.value.postal_code = m?.postal_code || "";
        formData.value.street = m?.street || "";
        formData.value.city_category_code = m?.city_category_code || "";

        formData.value.business_service_phone_number =
          m?.business_service_phone_number || "";
        formData.value.customer_service_phone_number =
          m?.customer_service_phone_number || "";
        formData.value.additional_contact_information =
          m?.additional_contact_information || "";
        formData.value.description = m?.description || "";

        formData.value.currency_code = m?.currency_code || "";
        formData.value.tax_id = m?.tax_id || "";
        formData.value.trade_register_number = m?.trade_register_number || "";
        formData.value.iban = m?.iban || "";
        formData.value.domiciliary_bank_number =
          m?.domiciliary_bank_number || "";

        formData.value.cut_off_time = hmsToTimeInput(m?.cut_off_time || "");
        formData.value.activation_flag = !!m?.activation_flag;
        formData.value.activation_time = isoToDatetimeLocal(
          m?.activation_time || ""
        );
        formData.value.activation_start_time = isoToDatetimeLocal(
          m?.activation_start_time || ""
        );
        formData.value.activation_end_time = isoToDatetimeLocal(
          m?.activation_end_time || ""
        );
      } catch {
        /* no-op */
      }
    };

    const fetchMerchantByName = async () => {
      const name = (formData.value.name || "").trim();
      if (!name) return;
      const exact = merchantsList.value.some(
        (n) => n.toLowerCase() === name.toLowerCase()
      );
      if (!exact) return;
      try {
        const resp = await fetch(
          `${API_BASE}/v1/merchants/${encodeURIComponent(name)}`,
          { method: "GET" }
        );
        if (!resp.ok) return;
        const data = await resp.json();
        const m = data?.merchant || null;
        if (m) fillFormFromMerchant(m);
      } catch {
        /* ignore */
      }
    };

    const isCurrentStepValid = computed(() => {
      switch (currentStep.value) {
        case 1:
          return !!formData.value.name;
        case 2:
        case 3:
        case 4:
          return true;
        default:
          return false;
      }
    });

    const nextStep = () => {
      if (currentStep.value < steps.value.length && isCurrentStepValid.value) {
        currentStep.value++;
      }
    };
    const previousStep = () => {
      if (currentStep.value > 1) {
        currentStep.value--;
      }
    };

    const chooseSuggestion = async () => {
      if (nameSuggestion.value) {
        formData.value.name = nameSuggestion.value;
        await fetchMerchantByName();
      }
    };

    const handleSubmit = async () => {
      submitError.value = "";
      responseData.value = null;
      if (!isCurrentStepValid.value) return;

      try {
        const details = {
          merchant_code: formData.value.merchant_code || undefined,
          legal_name: formData.value.legal_name || undefined,
          acceptor_name: formData.value.acceptor_name || undefined,
          acceptor_category_code:
            formData.value.acceptor_category_code || undefined,
          url: formData.value.url || undefined,
          language_code: formData.value.language_code || undefined,
          time_zone_id: formData.value.time_zone_id || undefined,
          country_code: formData.value.country_code || undefined,
          country_sub_division_code:
            formData.value.country_sub_division_code || undefined,
          home_country_code: formData.value.home_country_code || undefined,
          region_id: formData.value.region_id || undefined,
          city: formData.value.city || undefined,
          postal_code: formData.value.postal_code || undefined,
          street: formData.value.street || undefined,
          city_category_code: formData.value.city_category_code || undefined,
          business_service_phone_number:
            formData.value.business_service_phone_number || undefined,
          customer_service_phone_number:
            formData.value.customer_service_phone_number || undefined,
          additional_contact_information:
            formData.value.additional_contact_information || undefined,
          description: formData.value.description || undefined,
          currency_code: formData.value.currency_code || undefined,
          tax_id: formData.value.tax_id || undefined,
          trade_register_number:
            formData.value.trade_register_number || undefined,
          iban: formData.value.iban || undefined,
          domiciliary_bank_number:
            formData.value.domiciliary_bank_number || undefined,
          cut_off_time: toHHMMSS(formData.value.cut_off_time) || undefined,
          activation_flag: formData.value.activation_flag,
          activation_time: formData.value.activation_time || undefined,
          activation_start_time:
            formData.value.activation_start_time || undefined,
          activation_end_time: formData.value.activation_end_time || undefined,
        };

        const payload = {
          merchant_name: (formData.value.name || "").trim(),
          deep_scan: deepScanMode.value !== "none",
          details,
          start_date: formData.value.start_date || undefined,
          end_date: formData.value.end_date || undefined,
          preset_overrides_mode: deepScanMode.value, // optional hint
        };

        isSubmitting.value = true;

        const resp = await fetch(`${API_BASE}/v1/onboard`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok) {
          throw new Error(
            data?.detail || "Failed to submit onboarding request"
          );
        }
        const taskId = data?.task_id;
        if (!taskId) {
          throw new Error("No task_id returned from server");
        }
        pollTask(taskId);
      } catch (e) {
        isSubmitting.value = false;
        submitError.value = String(e?.message || e || "Unknown error");
        currentStep.value = 4;
      }
    };

    const pollTask = (taskId) => {
      if (pollTimer.value) {
        clearInterval(pollTimer.value);
        pollTimer.value = null;
      }
      pollTimer.value = setInterval(async () => {
        try {
          const r = await fetch(`${API_BASE}/v1/tasks/${taskId}`, {
            method: "GET",
          });
          const t = await r.json();
          if (t.status === "done") {
            clearInterval(pollTimer.value);
            pollTimer.value = null;
            isSubmitting.value = false;
            responseData.value = t.result || null;

            const m = responseData.value?.merchant;
            if (m?.merchant_id) formData.value.id = m.merchant_id;
            if (m?.merchant_name) formData.value.name = m.merchant_name;

            fetchMerchants();
            submitError.value = "";
            currentStep.value = 4;
          } else if (t.status === "error") {
            clearInterval(pollTimer.value);
            pollTimer.value = null;
            isSubmitting.value = false;
            submitError.value = t.error || "Task failed";
            currentStep.value = 4;
          }
        } catch {
          /* keep polling */
        }
      }, pollingIntervalMs.value);
    };

    const onNameBlur = async () => {
      await fetchMerchantByName();
    };

    onMounted(() => {
      const rng = defaultRange();
      formData.value.start_date = rng.start;
      formData.value.end_date = rng.end;
      fetchMerchants();
    });

    const resetToStart = () => {
      submitError.value = "";
      responseData.value = null;
      isSubmitting.value = false;
      const rng = defaultRange();
      formData.value = {
        id: "",
        name: "",
        start_date: rng.start,
        end_date: rng.end,
        merchant_code: "",
        legal_name: "",
        acceptor_name: "",
        acceptor_category_code: "",
        url: "",
        language_code: "",
        time_zone_id: "",
        country_code: "",
        country_sub_division_code: "",
        home_country_code: "",
        region_id: "",
        city: "",
        postal_code: "",
        street: "",
        city_category_code: "",
        business_service_phone_number: "",
        customer_service_phone_number: "",
        additional_contact_information: "",
        description: "",
        currency_code: "",
        tax_id: "",
        trade_register_number: "",
        iban: "",
        domiciliary_bank_number: "",
        cut_off_time: "",
        activation_flag: false,
        activation_time: "",
        activation_start_time: "",
        activation_end_time: "",
      };
      currentStep.value = 1;
      updateNameSuggestion("");
    };

    const viewMerchant = () => {
      const name = (
        responseData.value?.merchant?.merchant_name ||
        formData.value.name ||
        ""
      ).trim();
      if (!name) return;
      try {
        router.push(`/merchant/${encodeURIComponent(name)}`);
      } catch {
        // If router path differs, adjust here.
      }
    };

    watch(
      () => formData.value.name,
      (val) => updateNameSuggestion(val)
    );

    return {
      API_BASE,
      currentStep,
      steps,
      formData,
      deepScanMode,
      isCurrentStepValid,
      nextStep,
      previousStep,
      handleSubmit,
      merchantsList,
      nameSuggestion,
      chooseSuggestion,
      isNameUnique,
      isSubmitting,
      submitError,
      responseData,
      onNameBlur,
      resetToStart,
      viewMerchant,
      fmtDateTime,
    };
  },
};
</script>

<template>
  <div class="page-header">
    <nav class="breadcrumb">
      <router-link to="/dashboard" class="breadcrumb-item"
        >Dashboard</router-link
      >
      <span class="breadcrumb-separator">/</span>
      <span class="breadcrumb-item">Merchants</span>
      <span class="breadcrumb-separator">/</span>
      <span class="breadcrumb-item current">Onboarding</span>
    </nav>
  </div>

  <!-- Full-screen submission overlay -->
  <div v-if="isSubmitting" class="overlay">
    <div class="loader">
      <div class="spinner"></div>
      <div class="loader-text">Please wait, Processing...</div>
    </div>
  </div>

  <div class="progressive-form-container">
    <div class="form-card">
      <!-- Progress bar -->
      <div class="progress-header">
        <h2 class="form-title">Merchant Registration Form</h2>
        <div class="progress-bar">
          <div class="progress-steps">
            <div
              v-for="(step, index) in steps"
              :key="index"
              class="step"
              :class="{
                active: currentStep === index + 1,
                completed: currentStep > index + 1,
              }"
            >
              <div class="step-circle">
                <i v-if="currentStep > index + 1" class="check-icon">✓</i>
                <span v-else>{{ index + 1 }}</span>
              </div>
              <span class="step-label">{{ step.title }}</span>
            </div>
          </div>
          <div class="progress-line">
            <div
              class="progress-fill"
              :style="{
                width: `${((currentStep - 1) / (steps.length - 1)) * 100}%`,
              }"
            ></div>
          </div>
        </div>
      </div>

      <!-- Result step -->
      <div v-if="currentStep === 4" class="result-step">
        <div v-if="!submitError" class="success-card">
          <div class="success-icon">✓</div>
          <div class="success-text">
            <h3>Merchant onboarded successfully</h3>
          </div>
          <div class="success-details">
            <div class="detail-row">
              <span class="detail-label">Merchant ID:</span>
              <span class="detail-value">{{
                responseData?.merchant?.merchant_id || "-"
              }}</span>
            </div>
            <div class="detail-row">
              <span class="detail-label">Merchant Name:</span>
              <span class="detail-value">{{
                responseData?.merchant?.merchant_name || formData.name
              }}</span>
            </div>
            <div class="detail-row">
              <span class="detail-label">Active status:</span>
              <span class="detail-value">
                {{
                  responseData?.merchant?.activation_flag ??
                  formData.activation_flag
                    ? "Active"
                    : "Inactive"
                }}
              </span>
            </div>
            <div class="detail-row">
              <span class="detail-label">Added on:</span>
              <span class="detail-value">
                {{ fmtDateTime(responseData?.merchant?.created_at) }}
              </span>
            </div>
          </div>
          <div class="result-actions">

            <button class="btn btn-secondary" @click="resetToStart">
              New Merchantㅤ(+) 
            </button>
            <button class="btn btn-primary" @click="viewMerchant">
              View merchant
            </button>
          </div>
        </div>

        <div v-else class="error-card">
          <div class="error-title">Submission failed</div>
          <div class="error-text">{{ submitError }}</div>
          <div class="result-actions">
            <button class="btn btn-secondary" @click="previousStep">
              Back
            </button>
            <button class="btn btn-primary" @click="resetToStart">
              Start over
            </button>
          </div>
        </div>
      </div>

      <!-- Inline error on steps 1-3 -->
      <div v-if="submitError && currentStep !== 4" class="error-card-inline">
        <strong>Error:</strong> {{ submitError }}
      </div>

      <!-- Form (steps 1–3) -->
      <form
        v-if="currentStep !== 4"
        @submit.prevent="handleSubmit"
        class="form-content"
      >
        <!-- Step 1 -->
        <div v-if="currentStep === 1" class="form-step">
          <h3 class="step-title">Basic Information</h3>
          <div class="form-grid">
            <div class="form-group">
              <label for="id">ID</label>
              <input
                id="id"
                v-model="formData.id"
                type="text"
                maxlength="32"
                class="readonlymode form-input"
                placeholder="*Auto-Generated*"
                readonly
                disabled
              />
            </div>

            <div class="form-group name-col">
              <label for="name">Name *</label>
              <div class="name-input-wrapper">
                <input
                  id="name"
                  v-model="formData.name"
                  type="text"
                  required
                  maxlength="255"
                  class="form-input"
                  placeholder="Enter name"
                  autofocus
                  @blur="onNameBlur"
                />
                <!-- Chips aligned near name -->
                <div class="name-chips">
                  <span
                    v-if="formData.name"
                    class="chip"
                    :class="{ available: isNameUnique, taken: !isNameUnique }"
                  >
                    <template v-if="isNameUnique">Available</template>
                    <template v-else>Existing merchant</template>
                  </span>
                  <span
                    v-if="
                      nameSuggestion &&
                      nameSuggestion.toLowerCase() !==
                        (formData.name || '').toLowerCase()
                    "
                    class="chip suggestion"
                    @click="chooseSuggestion"
                    title="Click to use suggested name"
                  >
                    {{ nameSuggestion }}
                  </span>
                </div>
              </div>
            </div>

            <div class="form-group">
              <label for="merchant_code">Merchant Code</label>
              <input
                id="merchant_code"
                v-model="formData.merchant_code"
                type="text"
                maxlength="10"
                class="form-input"
                placeholder="8-digit or 6-digit + 00"
              />
            </div>

            <div class="form-group">
              <label for="legal_name">Legal Name</label>
              <input
                id="legal_name"
                v-model="formData.legal_name"
                type="text"
                maxlength="255"
                class="form-input"
                placeholder="Enter legal name"
              />
            </div>
            <div class="form-group">
              <label for="acceptor_name">Acceptor Name</label>
              <input
                id="acceptor_name"
                v-model="formData.acceptor_name"
                type="text"
                maxlength="255"
                class="form-input"
                placeholder="Enter acceptor name"
              />
            </div>
            <div class="form-group">
              <label for="acceptor_category_code">Acceptor Category Code</label>
              <input
                id="acceptor_category_code"
                v-model="formData.acceptor_category_code"
                type="text"
                maxlength="10"
                class="form-input"
                placeholder="e.g., 5411"
              />
            </div>
            <div class="form-group">
              <label for="url">Website URL</label>
              <input
                id="url"
                v-model="formData.url"
                type="url"
                maxlength="255"
                class="form-input"
                placeholder="https://merchant.example.com"
              />
            </div>
            <div class="form-group">
              <label for="language_code">Language Code</label>
              <select
                id="language_code"
                v-model="formData.language_code"
                class="form-input"
              >
                <option value="">Select language</option>
                <option value="en">English</option>
                <option value="es">Spanish</option>
                <option value="fr">French</option>
                <option value="de">German</option>
                <option value="it">Italian</option>
                <option value="pt">Portuguese</option>
              </select>
            </div>
            <div class="form-group">
              <label for="time_zone_id">Time Zone</label>
              <select
                id="time_zone_id"
                v-model="formData.time_zone_id"
                class="form-input"
              >
                <option value="">Select timezone</option>
                <option value="UTC">UTC</option>
                <option value="America/New_York">America/New_York</option>
                <option value="Europe/London">Europe/London</option>
                <option value="Europe/Paris">Europe/Paris</option>
                <option value="Asia/Tokyo">Asia/Tokyo</option>
              </select>
            </div>

            <div class="form-group">
              <label for="deep_scan_mode">Preset action</label>
              <select
                id="deep_scan_mode"
                v-model="deepScanMode"
                class="form-input"
              >
                <option value="none">None</option>
                <option value="redo">Redo preset</option>
                <option value="replace">Replace preset</option>
              </select>
            </div>

            <div class="form-group">
              <label for="start_date">Start Date (range)</label>
              <input
                id="start_date"
                v-model="formData.start_date"
                type="date"
                class="form-input"
              />
            </div>
            <div class="form-group">
              <label for="end_date">End Date (range)</label>
              <input
                id="end_date"
                v-model="formData.end_date"
                type="date"
                class="form-input"
              />
            </div>
          </div>
        </div>

        <!-- Step 2 -->
        <div v-if="currentStep === 2" class="form-step">
          <h3 class="step-title">Location & Contact Information</h3>
          <div class="form-grid">
            <div class="form-group">
              <label for="country_code">Country Code</label>
              <input
                id="country_code"
                v-model="formData.country_code"
                type="text"
                maxlength="5"
                class="form-input"
                placeholder="e.g., GB, US, DE"
              />
            </div>
            <div class="form-group">
              <label for="country_sub_division_code">State/Province Code</label>
              <input
                id="country_sub_division_code"
                v-model="formData.country_sub_division_code"
                type="text"
                maxlength="5"
                class="form-input"
                placeholder="e.g., LND, NY"
              />
            </div>
            <div class="form-group">
              <label for="home_country_code">Home Country Code</label>
              <input
                id="home_country_code"
                v-model="formData.home_country_code"
                type="text"
                maxlength="5"
                class="form-input"
                placeholder="e.g., GB"
              />
            </div>
            <div class="form-group">
              <label for="region_id">Region ID</label>
              <input
                id="region_id"
                v-model="formData.region_id"
                type="text"
                maxlength="20"
                class="form-input"
                placeholder="e.g., UK, EU, US"
              />
            </div>
            <div class="form-group">
              <label for="city">City</label>
              <input
                id="city"
                v-model="formData.city"
                type="text"
                maxlength="100"
                class="form-input"
                placeholder="Enter city"
              />
            </div>
            <div class="form-group">
              <label for="postal_code">Postal Code</label>
              <input
                id="postal_code"
                v-model="formData.postal_code"
                type="text"
                maxlength="20"
                class="form-input"
                placeholder="Enter postal code"
              />
            </div>
            <div class="form-group full-width">
              <label for="street">Street Address</label>
              <input
                id="street"
                v-model="formData.street"
                type="text"
                maxlength="255"
                class="form-input"
                placeholder="Enter street address"
              />
            </div>
            <div class="form-group">
              <label for="city_category_code">City Category Code</label>
              <input
                id="city_category_code"
                v-model="formData.city_category_code"
                type="text"
                maxlength="10"
                class="form-input"
                placeholder="e.g., URB, SUB, RUR"
              />
            </div>
            <div class="form-group">
              <label for="business_service_phone_number">Business Phone</label>
              <input
                id="business_service_phone_number"
                v-model="formData.business_service_phone_number"
                type="tel"
                maxlength="20"
                class="form-input"
                placeholder="e.g., +44-20-1234-5678"
              />
            </div>
            <div class="form-group">
              <label for="customer_service_phone_number"
                >Customer Service Phone</label
              >
              <input
                id="customer_service_phone_number"
                v-model="formData.customer_service_phone_number"
                type="tel"
                maxlength="20"
                class="form-input"
                placeholder="e.g., +44-20-9876-5432"
              />
            </div>
            <div class="form-group full-width">
              <label for="additional_contact_information"
                >Additional Contact Information</label
              >
              <textarea
                id="additional_contact_information"
                v-model="formData.additional_contact_information"
                class="form-input"
                rows="4"
                placeholder="Any additional contact information..."
              ></textarea>
            </div>
            <div class="form-group full-width">
              <label for="description">Description</label>
              <textarea
                id="description"
                v-model="formData.description"
                class="form-input"
                rows="3"
                placeholder="Merchant/acceptor profile notes"
              ></textarea>
            </div>
          </div>
        </div>

        <!-- Step 3 -->
        <div v-if="currentStep === 3" class="form-step">
          <h3 class="step-title">Financial & Operational Information</h3>
          <div class="form-grid">
            <div class="form-group">
              <label for="currency_code">Currency Code</label>
              <select
                id="currency_code"
                v-model="formData.currency_code"
                class="form-input"
              >
                <option value="">Select currency</option>
                <option value="USD">USD</option>
                <option value="EUR">EUR</option>
                <option value="GBP">GBP</option>
                <option value="CAD">CAD</option>
                <option value="JPY">JPY</option>
                <option value="AUD">AUD</option>
              </select>
            </div>
            <div class="form-group">
              <label for="tax_id">Tax ID</label>
              <input
                id="tax_id"
                v-model="formData.tax_id"
                type="text"
                maxlength="50"
                class="form-input"
                placeholder="e.g., TAX-ABCDEFG"
              />
            </div>
            <div class="form-group">
              <label for="trade_register_number">Trade Register Number</label>
              <input
                id="trade_register_number"
                v-model="formData.trade_register_number"
                type="text"
                maxlength="50"
                class="form-input"
                placeholder="e.g., REG-12345678"
              />
            </div>
            <div class="form-group">
              <label for="iban">IBAN</label>
              <input
                id="iban"
                v-model="formData.iban"
                type="text"
                maxlength="34"
                class="form-input"
                placeholder="International Bank Account Number"
              />
            </div>
            <div class="form-group">
              <label for="domiciliary_bank_number"
                >Domiciliary Bank Number</label
              >
              <input
                id="domiciliary_bank_number"
                v-model="formData.domiciliary_bank_number"
                type="text"
                maxlength="50"
                class="form-input"
                placeholder="e.g., BANK-123456"
              />
            </div>
            <div class="form-group">
              <label for="cut_off_time">Cut-off Time</label>
              <input
                id="cut_off_time"
                v-model="formData.cut_off_time"
                type="time"
                class="form-input"
              />
            </div>
            <div class="form-group">
              <label for="activation_flag">Activation Status</label>
              <select
                id="activation_flag"
                v-model="formData.activation_flag"
                class="form-input"
              >
                <option :value="true">Enabled</option>
                <option :value="false">Disabled</option>
              </select>
            </div>
            <div class="form-group">
              <label for="activation_time">Activation Time</label>
              <input
                id="activation_time"
                v-model="formData.activation_time"
                type="datetime-local"
                class="form-input"
              />
            </div>
            <div class="form-group">
              <label for="activation_start_time">Activation Start Time</label>
              <input
                id="activation_start_time"
                v-model="formData.activation_start_time"
                type="datetime-local"
                class="form-input"
              />
            </div>
            <div class="form-group">
              <label for="activation_end_time">Activation End Time</label>
              <input
                id="activation_end_time"
                v-model="formData.activation_end_time"
                type="datetime-local"
                class="form-input"
              />
            </div>
          </div>
        </div>

        <!-- Navigation -->
        <div class="form-navigation">
          <button
            v-if="currentStep > 1"
            type="button"
            @click="previousStep"
            class="btn btn-secondary"
          >
            Previous
          </button>
          <div class="spacer"></div>
          <button
            v-if="currentStep < 3"
            type="button"
            @click="nextStep"
            class="btn btn-primary"
            :disabled="!isCurrentStepValid"
          >
            Next
          </button>
          <button
            v-if="currentStep === 3"
            type="submit"
            class="btn btn-primary"
            :disabled="!isCurrentStepValid || isSubmitting"
          >
            Submit
          </button>
        </div>
      </form>
    </div>
  </div>
</template>

<style scoped>
.form-card {
  background: #fff;
  border: 1px solid #eaeaea;
  border-radius: 8px;
  margin: 18px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
}
.progress-header {
  margin-bottom: 12px;
}
.progress-steps {
  display: flex;
  gap: 16px;
  align-items: center;
  flex-wrap: wrap;
}
.step {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #f3f3f3ff;
}
.step.active .step-circle {
  border-color: #0b5fa4;
  color: #0b5fa4;
}
.step.completed .step-circle {
  background: #0b5fa4;
  color: #fff;
}
.step-circle {
  width: 28px;
  height: 28px;
  border-radius: 50%;
  border: 2px solid #ccc;
  display: flex;
  align-items: center;
  justify-content: center;
}
.progress-line {
  height: 6px;
  background: #eee;
  border-radius: 4px;
  margin-top: 8px;
}
.progress-fill {
  height: 6px;
  background: #0b5fa4;
  border-radius: 4px;
  transition: width 0.2s ease;
}

.form-content {
  margin-top: 16px;
}
.form-step {
  margin-bottom: 16px;
}
.form-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 12px;
}
.form-group.full-width {
  grid-column: 1 / -1;
}
.form-input {
  width: 100%;
  padding: 8px 10px;
  border: 1px solid #ddd;
  border-radius: 6px;
}
.readonlymode {
  background: #f9f9f9;
  color: #999;
}

.name-col {
  position: relative;
}
.name-input-wrapper {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.name-chips {
  display: flex;
  gap: 8px;
  align-items: center;
}
.chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 8px;
  border-radius: 12px;
  font-size: 12px;
  border: 1px solid #ddd;
  background: #f7f7f7;
  color: #444;
}
.chip.available {
  background: #e6f6ea;
  color: #1f7a33;
  border-color: #9fddb0;
}
.chip.taken {
  background: #fff4e6;
  color: #8a5700;
  border-color: #ffd195;
}
.chip.suggestion {
  background: #f5f8ff;
  cursor: pointer;
}

.btn {
  padding: 8px 14px;
  border: none;
  border-radius: 6px;
  cursor: pointer;
}
.btn-primary {
  background: #0b5fa4;
  color: #fff;
}
.btn-primary:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}
.btn-secondary {
  background: #777;
  color: #fff;
}
.spacer {
  flex: 1;
}

.overlay {
  position: fixed;
  inset: 0;
  background: rgba(255, 255, 255, 0.85);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}
.loader {
  text-align: center;
}
.spinner {
  width: 56px;
  height: 56px;
  border: 6px solid #008080c4  ;
  border-top-color: #cccccc86 ;
  border-radius: 50%;
  animation: spin 0.9s linear infinite;
  margin: 0 auto 10px;
}
.loader-text {
  color: #008080c4  ;
  font-weight: 600;
}
@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.result-step {
  margin-top: 16px;
}
.success-card {
    display: flex
;
    align-items: center;
    gap: 12px;

    border-radius: 8px;
    padding: 12px;
    margin-bottom: 1.7rem;
    flex-direction: column;
    justify-content: center;
    align-content: center;
    flex-wrap: nowrap;
    padding-top: 3rem;
}
.success-icon {
  width: 3.7rem;
  height: 3.7rem;
  font-size: 1.6rem;
  border-radius: 50%;
  background: #1f7a33;
  color: #fff;
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 700;
}
.success-text h3 {
  margin: 0;
  color: #1f7a33;
  margin-top: 1rem;
  font-size: 1.2rem;
  margin-bottom: 2rem;
  text-decoration: underline;
}
.success-details {
  margin-bottom: 3rem;
}
.detail-row {
display: flex
;
    gap: 8px;
    width: 17rem;
    align-items: flex-start;
    justify-content: space-between;
    flex-wrap: nowrap;
    flex-direction: row;
}
.detail-label {
  font-weight: 600;
  color: #333;
}
.detail-value {
  color: #0b5fa4;
}
.result-actions {
  margin-top: 12px;
  display: flex;
  gap: 1rem;
}

.error-card {
  border: 1px solid #f5c0c0;
  background: #fff5f5;
  color: #7a1f1f;
  border-radius: 8px;
  padding: 12px;
}
.error-title {
  font-weight: 700;
  margin-bottom: 6px;
}
.error-text {
  margin-bottom: 12px;
}
.error-card-inline {
  border: 1px solid #f5c0c0;
  background: #fff5f5;
  color: #7a1f1f;
  border-radius: 8px;
  padding: 10px;
  margin-bottom: 10px;
}
</style>

<style scoped>
.progressive-form-container {
  min-height: 100vh;
  background: linear-gradient(135deg, #ffffffff 0%, #acb9ce4f 100%);
  padding: 2rem;
  display: flex;
  justify-content: center;
  align-items: flex-start;
  font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
}

.form-card {
  background: white;
  border-radius: 16px;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
  max-width: 900px;
  width: 100%;
  overflow: hidden;
}

.progress-header {
  background: linear-gradient(135deg, #008080c4 0%, #008080d7 100%);
  color: white;
  padding: 2rem;
  text-align: center;
}

.form-title {
  margin: 0 0 1.8rem 0;
  font-size: 2rem;
  font-weight: 300;
}

.progress-bar {
  position: relative;
}

.progress-steps {
  display: flex;
  justify-content: space-between;
  position: relative;
  z-index: 2;
  padding-top: 8px;
}

.step {
  display: flex;
  flex-direction: column;
  align-items: center;
  flex: 1;
}

.step-circle {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  background: rgba(255, 255, 255, 0.3);
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: bold;
  margin-bottom: 0.5rem;
  transition: all 0.3s ease;
  border: 2px solid rgba(255, 255, 255, 0.5);
}

.step.active .step-circle {
  background: white;
  color: #008080;
  border-color: white;
}

.step.completed .step-circle {
  background: #20b2aa;
  color: white;
  border-color: #20b2aa;
}

.check-icon {
  font-size: 1.2rem;
}

.step-label {
  font-size: 0.9rem;
  opacity: 0.8;
}

.step.active .step-label {
  opacity: 1;
  font-weight: 500;
}

.progress-line {
  position: absolute;
  top: 20px;
  left: 0;
  right: 0;
  height: 2px;
  background: rgba(255, 255, 255, 0.3);
  z-index: 1;
}

.progress-fill {
  height: 100%;
  background: white;
  transition: width 0.3s ease;
}

.form-content {
  padding: 2.4rem;
  padding-top: 1.3rem;
}

.form-step {
  min-height: 400px;
}

.step-title {
  color: #333;
  margin-bottom: 3rem;
  font-size: 1.5rem;
  font-weight: 400;
  text-align: center;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 1.5rem;
}

.form-group {
  display: flex;
  flex-direction: column;
}

.form-group.full-width {
  grid-column: 1 / -1;
}

.form-group label {
  margin-bottom: 0.5rem;
  color: #555;
  font-weight: 500;
  font-size: 0.9rem;
}

.form-input {
  padding: 0.75rem 1rem;
  border: 2px solid #e1e5e9;
  border-radius: 8px;
  font-size: 1rem;
  transition: all 0.3s ease;
  background: #fafbfc;
}

.form-input:focus {
  outline: none;
  border-color: #008080;
  background: white;
  box-shadow: 0 0 0 3px rgba(0, 128, 128, 0.1);
}

.form-input:invalid {
  border-color: #e74c3c;
}

textarea.form-input {
  resize: vertical;
  min-height: 100px;
}

/* readonly */
.readonlymode:read-only {
  background: #d3d3d3ff;
  cursor: not-allowed;
}

.checkbox-wrapper {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0.5rem;
}

.form-checkbox {
  width: 18px;
  height: 18px;
  margin: 0;
}

.checkbox-label {
  margin: 0;
  cursor: pointer;
}

.form-navigation {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 3rem;
  padding-top: 2rem;
  border-top: 1px solid #e1e5e9;
}

.spacer {
  flex: 1;
}

.btn {
  padding: 0.75rem 2rem;
  border: none;
  border-radius: 8px;
  font-size: 1rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.3s ease;
  min-width: 120px;
}

.btn-primary {
  background: #008080;
  color: white;
}

.btn-primary:hover:not(:disabled) {
  background: #006666;
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 128, 128, 0.3);
}

.btn-secondary {
  background: #6c757d;
  color: white;
}

.btn-secondary:hover {
  background: #5a6268;
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(108, 117, 125, 0.3);
}

.btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
  transform: none !important;
  box-shadow: none !important;
}

.page-header {
  padding: 16px 20px;
  background: #ffffff;
  border-bottom: 1px solid #e5e7eb;
}

.breadcrumb {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 14px;
  color: #6b7280;
}

.breadcrumb-item {
  color: #6b7280;
  text-decoration: none;
  transition: color 0.2s ease;
}

.breadcrumb-item:hover {
  color: #008080;
}

.breadcrumb-item.current {
  color: #008080;
  font-weight: 500;
}

.breadcrumb-separator {
  color: #d1d5db;
  user-select: none;
}

@media (max-width: 768px) {
  .progressive-form-container {
    padding: 1rem;
  }

  .form-grid {
    grid-template-columns: 1fr;
  }

  .progress-steps {
    flex-direction: column;
    gap: 1rem;
  }

  .step {
    flex-direction: row;
    justify-content: flex-start;
    gap: 1rem;
  }

  .step-circle {
    margin-bottom: 0;
    width: 35px;
    height: 35px;
  }

  .progress-line {
    display: none;
  }
}
</style>
