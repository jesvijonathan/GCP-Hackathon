<template>
  <div class="page-header">
    <nav class="breadcrumb">
      <router-link to="/dashboard" class="breadcrumb-item">Dashboard</router-link>
      <span class="breadcrumb-separator">/</span>
      <span class="breadcrumb-item">Merchants</span>
      <span class="breadcrumb-separator">/</span>
      <span class="breadcrumb-item current">
        {{ merchant?.name || routeIdentifier || "Loading..." }}
      </span>
    </nav>
  </div>

  <div class="merchant-detail-container">
    <div v-if="loading" class="loading">Loading merchant details...</div>

    <div v-if="merchant && !loading" class="layout">
      <!-- Side summary panel -->
      <aside class="side-panel">
        <div class="merchant-header">
          <div class="title-row">
            <span class="status-dot" :class="{ active: isActive, inactive: !isActive }"></span>
            <h1 class="merchant-title">{{ merchant.name || "Merchant" }}</h1>
          </div>
          <p class="merchant-id">ID: {{ merchant.id || "-" }}</p>
        </div>

        <div class="summary-list">
          <div class="summary-item">
            <span class="label">Region</span>
            <span class="value">{{ merchant.region_id || "-" }}</span>
          </div>
          <div class="summary-item">
            <span class="label">Timezone</span>
            <span class="value">{{ merchant.time_zone_id || "-" }}</span>
          </div>
          <div class="summary-item">
            <span class="label">Status</span>
            <span class="value">{{ isActive ? "Active" : "Inactive" }}</span>
          </div>
        </div>

        <!-- Navigation / Notifications -->
        <div class="panel-section">
          <div class="panel-actions">
            <div @click="openEmailBox" class="notification">Notifications</div>
            <div @click="openSocialUpdates" class="notification">Social Updates</div>
            <div @click="openExplore" class="notification">Explore</div>
          </div>
        </div>

        <!-- Auto refresh -->
        <div class="panel-section">
          <div class="action-center-dropdown">
            <label class="checkbox-label">
              <input v-model="autoRefresh" type="checkbox" class="form-checkbox" />
              Auto refresh
            </label>
            <div v-if="autoRefresh" class="form-group">
              <label for="refreshSec">Refresh interval (seconds)</label>
              <input id="refreshSec" v-model.number="refreshSec" type="number" min="2" class="form-input" />
            </div>
          </div>
        </div>

        <!-- Admin actions -->
        <div class="panel-section action-center-dropdown">
          <button @click="openPermanentBanModal">Permanent Ban</button>
          <button @click="openShadowBanModal">Shadow Ban</button>
          <button @click="openRestrictModal">Restrict Merchant</button>
        </div>

        <!-- Save -->
        <div class="panel-section action-center-dropdown">
          <button class="btn btn-primary primary-btn" @click="saveUpdates" :disabled="!hasChanges || saving">
            {{ saving ? "Saving..." : "Update Changes" }}
          </button>
        </div>

        <div class="actions-taken">
          <h1>Actions Taken On {{ merchant.name }}</h1>
          <p class="actions-taken-data">
            {{ actionsTaken.length > 0 ? actionsTaken.join(", ") : "No actions taken" }}
          </p>
        </div>
      </aside>

      <!-- Main cards -->
      <main class="merchant-detail">
        <!-- Overview & Identifiers -->
        <section class="card">
          <div class="card-header">
            <h3>Overview & Identifiers</h3>
          </div>
          <div class="card-body">
            <p class="row-display"><strong>Merchant Name:</strong> <span>{{ merchant.name || "N/A" }}</span></p>
            <p class="row-display"><strong>Merchant ID:</strong> <span>{{ merchant.id || "N/A" }}</span></p>
            <FieldRow label="Merchant Code" field="merchant_code" v-model="merchant.merchant_code" :edit.sync="editFields.merchant_code" />
            <FieldRow label="Acceptor Name" field="acceptor_name" v-model="merchant.acceptor_name" :edit.sync="editFields.acceptor_name" />
            <FieldRow label="Acceptor Category Code (MCC)" field="acceptor_category_code" v-model="merchant.acceptor_category_code" :edit.sync="editFields.acceptor_category_code" />
          </div>
        </section>

        <!-- Business & Web -->
        <section class="card">
          <div class="card-header">
            <h3>Business & Web</h3>
          </div>
          <div class="card-body">
            <FieldRow label="Website URL" field="url" v-model="merchant.url" :edit.sync="editFields.url" />
            <div class="desc-row">
              <label class="desc-label">Description</label>
              <div class="desc-content">
                <div v-if="!editFields.description" class="desc-text">
                  {{ merchant.description || "No description provided." }}
                </div>
                <div v-else class="desc-edit">
                  <textarea v-model="merchant.description" class="form-textarea" rows="3" placeholder="Merchant profile notes"></textarea>
                </div>
              </div>
              <button class="edit-btn" @click="editFields.description = !editFields.description" title="Edit">✎</button>
            </div>
            <FieldRow label="Trade Register Number" field="trade_register_number" v-model="merchant.trade_register_number" :edit.sync="editFields.trade_register_number" />
            <FieldRow label="Legal Name" field="legal_name" v-model="merchant.legal_name" :edit.sync="editFields.legal_name" />
            <FieldRow label="Language Code" field="language_code" v-model="merchant.language_code" :edit.sync="editFields.language_code" />
            <FieldRow label="Time Zone" field="time_zone_id" v-model="merchant.time_zone_id" :edit.sync="editFields.time_zone_id" />
          </div>
        </section>

        <!-- Location -->
        <section class="card">
          <div class="card-header">
            <h3>Location</h3>
          </div>
          <div class="card-body">
            <FieldRow label="Country Code" field="country_code" v-model="merchant.country_code" :edit.sync="editFields.country_code" />
            <FieldRow label="Home Country Code" field="home_country_code" v-model="merchant.home_country_code" :edit.sync="editFields.home_country_code" />
            <FieldRow label="Region ID" field="region_id" v-model="merchant.region_id" :edit.sync="editFields.region_id" />
            <FieldRow label="State/Province (Subdivision)" field="country_sub_division_code" v-model="merchant.country_sub_division_code" :edit.sync="editFields.country_sub_division_code" />
            <FieldRow label="City" field="city" v-model="merchant.city" :edit.sync="editFields.city" />
            <FieldRow label="Street" field="street" v-model="merchant.street" :edit.sync="editFields.street" />
            <FieldRow label="Postal Code" field="postal_code" v-model="merchant.postal_code" :edit.sync="editFields.postal_code" />
            <FieldRow label="City Category Code" field="city_category_code" v-model="merchant.city_category_code" :edit.sync="editFields.city_category_code" />
            <p class="row-display"><strong>Address:</strong> <span>{{ fullAddress || "N/A" }}</span></p>
          </div>
        </section>

        <!-- Contact -->
        <section class="card">
          <div class="card-header">
            <h3>Contact</h3>
          </div>
          <div class="card-body">
            <FieldRow label="Business Service Phone" field="business_service_phone_number" v-model="merchant.business_service_phone_number" :edit.sync="editFields.business_service_phone_number" />
            <FieldRow label="Customer Service Phone" field="customer_service_phone_number" v-model="merchant.customer_service_phone_number" :edit.sync="editFields.customer_service_phone_number" />
            <FieldRow label="Additional Contact Information" field="additional_contact_information" v-model="merchant.additional_contact_information" :edit.sync="editFields.additional_contact_information" type="textarea" />
          </div>
        </section>

        <!-- Finance & Banking -->
        <section class="card">
          <div class="card-header">
            <h3>Finance & Banking</h3>
          </div>
          <div class="card-body">
            <div class="finance-badges">
              <div><strong>Country:</strong> <span>{{ merchant.country_code || "-" }}</span></div>
              <div><strong>Currency:</strong> <span>{{ merchant.currency_code || "-" }}</span></div>
              <div><strong>Category:</strong> <span>{{ merchant.acceptor_category_code || "-" }} {{ mccLabel }}</span></div>
            </div>

            <FieldRow label="Currency Code" field="currency_code" v-model="merchant.currency_code" :edit.sync="editFields.currency_code" />
            <FieldRow label="Tax ID" field="tax_id" v-model="merchant.tax_id" :edit.sync="editFields.tax_id" />
            <FieldRow label="IBAN" field="iban" v-model="merchant.iban" :edit.sync="editFields.iban" />
            <FieldRow label="Domiciliary Bank Number" field="domiciliary_bank_number" v-model="merchant.domiciliary_bank_number" :edit.sync="editFields.domiciliary_bank_number" />
            <FieldRow label="Cut-off Time" field="cut_off_time" v-model="merchant.cut_off_time" :edit.sync="editFields.cut_off_time" type="time" />
          </div>
        </section>

        <!-- Activation & Dates -->
        <section class="card">
          <div class="card-header">
            <h3>Activation & Dates</h3>
          </div>
          <div class="card-body grid-2">
            <FieldRow label="Activation Status" field="activation_flag" v-model="merchant.activation_flag" :edit.sync="editFields.activation_flag" type="select-bool" />
            <FieldRow label="Activation Time" field="activation_time" v-model="merchant.activation_time" :edit.sync="editFields.activation_time" type="datetime-local" />
            <FieldRow label="Activation Start Time" field="activation_start_time" v-model="merchant.activation_start_time" :edit.sync="editFields.activation_start_time" type="datetime-local" />
            <FieldRow label="Activation End Time" field="activation_end_time" v-model="merchant.activation_end_time" :edit.sync="editFields.activation_end_time" type="datetime-local" />
            <FieldRow label="Start Date (range)" field="start_date" v-model="merchant.start_date" :edit.sync="editFields.start_date" type="date" />
            <FieldRow label="End Date (range)" field="end_date" v-model="merchant.end_date" :edit.sync="editFields.end_date" type="date" />
          </div>
          <div class="card-footer meta-footer">
            <div><strong>Created:</strong> {{ formatDate(merchant.created_at) }}</div>
            <div><strong>Updated:</strong> {{ formatDate(merchant.updated_at) }}</div>
          </div>
        </section>
      </main>
    </div>

    <div v-else-if="!loading" class="error">
      <h2>Merchant not found</h2>
      <p>The merchant "{{ routeIdentifier }}" could not be found.</p>
      <button @click="goBack" class="btn btn-secondary">← Back to Dashboard</button>
    </div>

    <!-- Permanent Ban Modal -->
    <div v-if="showPermanentBanModal" class="modal-overlay" @click="closePermanentBanModal">
      <div class="modal-content" @click.stop>
        <div class="modal-header">
          <h3>Confirm Permanent Ban</h3>
          <button @click="closePermanentBanModal" class="close-btn">&times;</button>
        </div>
        <div class="modal-body">
          <div class="warning-icon">⚠️</div>
          <p class="warning-text">Are you sure you want to permanently ban <strong>{{ merchant?.name }}</strong>?</p>
          <p class="warning-subtext">This action cannot be undone.</p>
        </div>
        <div class="modal-footer">
          <button @click="closePermanentBanModal" class="btn-cancel">Cancel</button>
          <button @click="confirmPermanentBan" class="btn-danger">Permanent Ban</button>
        </div>
      </div>
    </div>

    <!-- Shadow Ban Modal -->
    <div v-if="showShadowBanModal" class="modal-overlay" @click="closeShadowBanModal">
      <div class="modal-content" @click.stop>
        <div class="modal-header">
          <h3>Confirm Shadow Ban</h3>
          <button @click="closeShadowBanModal" class="close-btn">&times;</button>
        </div>
        <div class="modal-body">
          <div class="warning-icon">🔒</div>
          <p class="warning-text">Are you sure you want to shadow ban <strong>{{ merchant?.name }}</strong>?</p>
          <p class="warning-subtext">Limited visibility and functionality.</p>
        </div>
        <div class="modal-footer">
          <button @click="closeShadowBanModal" class="btn-cancel">Cancel</button>
          <button @click="confirmShadowBan" class="btn-warning">Shadow Ban</button>
        </div>
      </div>
    </div>

    <!-- Restrict Modal -->
    <div v-if="showRestrictModal" class="modal-overlay" @click="closeRestrictModal">
      <div class="modal-content large" @click.stop>
        <div class="modal-header">
          <h3>Restrict Merchant: {{ merchant?.name }}</h3>
          <button @click="closeRestrictModal" class="close-btn">&times;</button>
        </div>
        <div class="modal-body">
          <form @submit.prevent="confirmRestriction" class="restriction-form">
            <div class="form-section">
              <h4>Transaction Limits</h4>
              <div class="form-row">
                <div class="form-group">
                  <label for="dailyLimit">Daily Transaction Limit ($)</label>
                  <input id="dailyLimit" v-model.number="restrictionData.dailyTransactionLimit" type="number" min="0" class="form-input" />
                </div>
                <div class="form-group">
                  <label for="monthlyLimit">Monthly Transaction Limit ($)</label>
                  <input id="monthlyLimit" v-model.number="restrictionData.monthlyTransactionLimit" type="number" min="0" class="form-input" />
                </div>
              </div>
            </div>

            <div class="form-section">
              <h4>Restricted Categories (MCC)</h4>
              <div class="form-group">
                <div class="category-selector">
                  <div class="selected-categories">
                    <span v-for="code in restrictionData.restrictedCategoryCodes" :key="code" class="category-tag">
                      {{ code }}
                      <button type="button" @click="removeCategoryCode(code)" class="remove-tag">&times;</button>
                    </span>
                  </div>
                  <select v-model="selectedCategoryCode" @change="addCategoryCode" class="form-select">
                    <option value="">Select category</option>
                    <option value="5541">5541 Gas Stations</option>
                    <option value="5542">5542 Automated Fuel</option>
                    <option value="5812">5812 Restaurants</option>
                    <option value="5814">5814 Fast Food</option>
                    <option value="5921">5921 Liquor Stores</option>
                    <option value="5993">5993 Cigar Stores</option>
                    <option value="6010">6010 Financial Institutions</option>
                    <option value="6011">6011 ATMs</option>
                    <option value="7995">7995 Gambling</option>
                  </select>
                </div>
              </div>
            </div>

            <div class="form-section">
              <h4>Additional Restrictions</h4>
              <div class="form-group">
                <label for="maxTransactionAmount">Max Single Transaction ($)</label>
                <input id="maxTransactionAmount" v-model.number="restrictionData.maxTransactionAmount" type="number" min="0" class="form-input" />
              </div>
              <div class="form-group">
                <label for="allowedCountries">Allowed Countries</label>
                <input id="allowedCountries" v-model="restrictionData.allowedCountries" type="text" class="form-input" placeholder="e.g., US, CA, GB" />
              </div>
              <div class="form-group">
                <label class="checkbox-label">
                  <input v-model="restrictionData.requireAdditionalVerification" type="checkbox" class="form-checkbox" />
                  Require additional verification
                </label>
              </div>
              <div class="form-group">
                <label class="checkbox-label">
                  <input v-model="restrictionData.blockInternationalTransactions" type="checkbox" class="form-checkbox" />
                  Block international transactions
                </label>
              </div>
            </div>

            <div class="form-section">
              <h4>Reason</h4>
              <div class="form-group">
                <label for="reason">Reason</label>
                <textarea id="reason" v-model="restrictionData.reason" rows="3" class="form-textarea" placeholder="Explain the reason..." required></textarea>
              </div>
            </div>

            <div class="form-section">
              <h4>Duration</h4>
              <div class="form-row">
                <div class="form-group">
                  <label for="startDate">Start Date</label>
                  <input id="startDate" v-model="restrictionData.startDate" type="date" class="form-input" required />
                </div>
                <div class="form-group">
                  <label for="endDate">End Date (optional)</label>
                  <input id="endDate" v-model="restrictionData.endDate" type="date" class="form-input" />
                </div>
              </div>
            </div>
          </form>
        </div>
        <div class="modal-footer">
          <button @click="closeRestrictModal" class="btn-cancel">Cancel</button>
          <button @click="confirmRestriction" class="btn-primary">Apply Restrictions</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, reactive, computed, onMounted, watch, nextTick } from "vue";
import { useRoute, useRouter } from "vue-router";
import { toast } from "vue3-toastify";
import "vue3-toastify/dist/index.css";
// import twitterAnal from './twitterAnal.vue';

export default {
  name: "MerchantDetail",
  components: {
    FieldRow: {
      name: "FieldRow",
      props: {
        label: { type: String, required: true },
        field: { type: String, required: true },
        modelValue: { required: false },
        edit: { type: Boolean, default: false },
        type: { type: String, default: "text" }, // text | textarea | date | time | datetime-local | select-bool
      },
      emits: ["update:modelValue", "update:edit"],
      setup(props, { emit }) {
        const localVal = ref(props.modelValue);
        const inputRef = ref(null);

        watch(
          () => props.modelValue,
          (v) => { localVal.value = v; }
        );

        watch(
          () => props.edit,
          async (isEdit) => {
            if (isEdit) {
              await nextTick();
              if (inputRef.value) {
                inputRef.value.focus();
                const el = inputRef.value;
                if (el.setSelectionRange) {
                  const len = String(el.value || "").length;
                  el.setSelectionRange(len, len);
                }
              }
            }
          }
        );

        const toggleEdit = () => emit("update:edit", !props.edit);
        const updateVal = (e) => {
          let val = e && e.target ? e.target.value : localVal.value;
          if (props.type === "select-bool") {
            val = val === true || val === "true";
          }
          emit("update:modelValue", val);
        };

        return { localVal, toggleEdit, updateVal, inputRef };
      },
      template: `
        <div class="field-row">
          <div class="field-label">{{ label }}</div>
          <div class="field-value">
            <template v-if="!edit">
              <span>{{ modelValue ?? 'N/A' }}</span>
            </template>
            <div v-else class="edit-input">
              <textarea v-if="type==='textarea'" v-model="localVal" class="form-textarea" rows="3" @input="updateVal"></textarea>
              <input v-else-if="type==='date'" type="date" :value="modelValue || ''" @input="updateVal" ref="inputRef" class="form-input" />
              <input v-else-if="type==='time'" type="time" :value="modelValue || ''" @input="updateVal" ref="inputRef" class="form-input" />
              <input v-else-if="type==='datetime-local'" type="datetime-local" :value="modelValue || ''" @input="updateVal" ref="inputRef" class="form-input" />
              <select v-else-if="type==='select-bool'" :value="modelValue" @change="updateVal" ref="inputRef" class="form-input">
                <option :value="true">Enabled</option>
                <option :value="false">Disabled</option>
              </select>
              <input v-else :value="modelValue || ''" @input="updateVal" ref="inputRef" class="form-input" />
            </div>
          </div>
          <button class="edit-btn" @click="toggleEdit" title="Edit">{{ edit ? '✓' : '✎' }}</button>
        </div>
      `,
    },
  },
  setup() {
    const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
    const route = useRoute();
    const router = useRouter();

    const routeName = computed(() => {
      return (route.params.name && String(route.params.name)) ||
             (route.query.merchant && String(route.query.merchant)) || "";
    });
    const routeId = computed(() => {
      return (route.params.id && String(route.params.id)) ||
             (route.query.id && String(route.query.id)) || "";
    });
    const routeIdentifier = computed(() => routeName.value || routeId.value || "");

    const merchant = reactive({
      id: "", name: "",
      url: "", language_code: "", time_zone_id: "",
      legal_name: "", acceptor_name: "", acceptor_category_code: "",
      country_code: "", country_sub_division_code: "", home_country_code: "", region_id: "",
      city: "", postal_code: "", street: "", city_category_code: "",
      business_service_phone_number: "", customer_service_phone_number: "", additional_contact_information: "",
      currency_code: "", tax_id: "", trade_register_number: "", iban: "", domiciliary_bank_number: "", cut_off_time: "",
      activation_flag: false, activation_time: "", activation_start_time: "", activation_end_time: "",
      start_date: "", end_date: "", created_at: "", updated_at: "",
      merchant_code: "", description: "",
      status: "Unknown",
    });

    const originalDoc = ref(null);
    const editFields = reactive({});
    [
      "merchant_code","url","language_code","time_zone_id","legal_name","acceptor_name","acceptor_category_code",
      "country_code","country_sub_division_code","home_country_code","region_id","city","postal_code","street","city_category_code",
      "business_service_phone_number","customer_service_phone_number","additional_contact_information",
      "currency_code","tax_id","trade_register_number","iban","domiciliary_bank_number","cut_off_time",
      "activation_flag","activation_time","activation_start_time","activation_end_time",
      "start_date","end_date","description"
    ].forEach(k => editFields[k] = false);

    const loading = ref(true);
    const saving = ref(false);
    const actionsTaken = ref([]);
    const showPermanentBanModal = ref(false);
    const showShadowBanModal = ref(false);
    const showRestrictModal = ref(false);

    const restrictionData = reactive({
      dailyTransactionLimit: null,
      monthlyTransactionLimit: null,
      maxTransactionAmount: null,
      restrictedCategoryCodes: [],
      allowedCountries: "",
      requireAdditionalVerification: false,
      blockInternationalTransactions: false,
      reason: "",
      startDate: new Date().toISOString().split("T")[0],
      endDate: "",
    });
    const selectedCategoryCode = ref("");

    const autoRefresh = ref(false);
    const refreshSec = ref(10);
    let refreshTimer = null;

    watch(autoRefresh, (on) => {
      if (refreshTimer) {
        clearInterval(refreshTimer);
        refreshTimer = null;
      }
      if (on) {
        refreshTimer = setInterval(() => {
          loadFromRoute();
        }, Math.max(2000, (refreshSec.value || 10) * 1000));
      }
    });
    watch(refreshSec, () => {
      if (autoRefresh.value) {
        autoRefresh.value = false;
        autoRefresh.value = true;
      }
    });

    const isActive = computed(() => !!merchant.activation_flag);

    const mccMap = {
      "5411": "Grocery Stores",
      "5812": "Restaurants",
      "5999": "Misc Retail",
      "5732": "Electronics",
      "4789": "Transportation",
      "7399": "Business Services",
      "4814": "Telecom",
      "5699": "Apparel",
      "4900": "Utilities",
      "5921": "Liquor Stores",
      "5813": "Bars/Clubs",
      "6051": "Prepaid",
      "6011": "ATMs",
      "7995": "Gambling",
    };
    const mccLabel = computed(() => {
      const code = merchant.acceptor_category_code;
      return code && mccMap[code] ? `(${mccMap[code]})` : "";
    });

    const formatDate = (iso) => {
      if (!iso) return "N/A";
      try {
        const d = new Date(iso);
        return d.toLocaleDateString("en-US", {
          year: "numeric", month: "long", day: "numeric", hour: "2-digit", minute: "2-digit",
        });
      } catch {
        return iso;
      }
    };

    const fullAddress = computed(() => {
      return [merchant.street, merchant.city, merchant.postal_code, merchant.country_code].filter(Boolean).join(", ");
    });

    const toHHMMSS = (val) => {
      if (!val) return null;
      const p = String(val).split(":");
      const hh = (p[0] || "00").padStart(2, "0");
      const mm = String(p[1] || "00").padStart(2, "0");
      const ss = String(p[2] || "00").padStart(2, "0");
      return `${hh}:${mm}:${ss}`;
    };

    const isoToLocal = (iso) => {
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

    const hasChanges = computed(() => {
      const doc = originalDoc.value;
      if (!doc) return false;
      const cmp = (a, b) => ((a ?? null) !== (b ?? null));

      if (cmp(merchant.merchant_code, doc.merchant_code)) return true;
      if (cmp(merchant.url, doc.url)) return true;
      if (cmp(merchant.language_code, doc.language_code)) return true;
      if (cmp(merchant.time_zone_id, doc.time_zone_id)) return true;

      if (cmp(merchant.legal_name, doc.legal_name)) return true;
      if (cmp(merchant.acceptor_name, doc.acceptor_name)) return true;
      if (cmp(merchant.acceptor_category_code, doc.acceptor_category_code)) return true;

      if (cmp(merchant.country_code, doc.country_code)) return true;
      if (cmp(merchant.country_sub_division_code, doc.country_sub_division_code)) return true;
      if (cmp(merchant.home_country_code, doc.home_country_code)) return true;
      if (cmp(merchant.region_id, doc.region_id)) return true;
      if (cmp(merchant.city, doc.city)) return true;
      if (cmp(merchant.postal_code, doc.postal_code)) return true;
      if (cmp(merchant.street, doc.street)) return true;
      if (cmp(merchant.city_category_code, doc.city_category_code)) return true;

      if (cmp(merchant.business_service_phone_number, doc.business_service_phone_number)) return true;
      if (cmp(merchant.customer_service_phone_number, doc.customer_service_phone_number)) return true;
      if (cmp(merchant.additional_contact_information, doc.additional_contact_information)) return true;

      if (cmp(merchant.currency_code, doc.currency_code)) return true;
      if (cmp(merchant.tax_id, doc.tax_id)) return true;
      if (cmp(merchant.trade_register_number, doc.trade_register_number)) return true;
      const ibanNorm = merchant.iban === "" ? null : merchant.iban;
      if (cmp(ibanNorm, doc.iban ?? null)) return true;
      if (cmp(merchant.domiciliary_bank_number, doc.domiciliary_bank_number)) return true;
      if (cmp(toHHMMSS(merchant.cut_off_time || ""), doc.cut_off_time)) return true;

      if (cmp(!!merchant.activation_flag, !!doc.activation_flag)) return true;
      const actTimeISO = merchant.activation_time ? new Date(merchant.activation_time).toISOString() : null;
      const actStartISO = merchant.activation_start_time ? new Date(merchant.activation_start_time).toISOString() : null;
      const actEndISO = merchant.activation_end_time ? new Date(merchant.activation_end_time).toISOString() : null;
      if (cmp(actTimeISO, doc.activation_time || null)) return true;
      if (cmp(actStartISO, doc.activation_start_time || null)) return true;
      if (cmp(actEndISO, doc.activation_end_time || null)) return true;

      if (cmp(merchant.start_date, doc.start_date)) return true;
      if (cmp(merchant.end_date, doc.end_date)) return true;
      if (cmp(merchant.description, doc.description)) return true;

      return false;
    });

    const mapDocToView = (doc) => {
      originalDoc.value = doc;

      merchant.id = doc.merchant_id || "";
      merchant.name = doc.merchant_name || "";

      merchant.merchant_code = doc.merchant_code || "";
      merchant.url = doc.url || "";
      merchant.language_code = doc.language_code || "";
      merchant.time_zone_id = doc.time_zone_id || "";

      merchant.legal_name = doc.legal_name || "";
      merchant.acceptor_name = doc.acceptor_name || "";
      merchant.acceptor_category_code = doc.acceptor_category_code || "";

      merchant.country_code = doc.country_code || "";
      merchant.country_sub_division_code = doc.country_sub_division_code || "";
      merchant.home_country_code = doc.home_country_code || "";
      merchant.region_id = doc.region_id || "";
      merchant.city = doc.city || "";
      merchant.postal_code = doc.postal_code || "";
      merchant.street = doc.street || "";
      merchant.city_category_code = doc.city_category_code || "";

      merchant.business_service_phone_number = doc.business_service_phone_number || "";
      merchant.customer_service_phone_number = doc.customer_service_phone_number || "";
      merchant.additional_contact_information = doc.additional_contact_information || "";

      merchant.currency_code = doc.currency_code || "";
      merchant.tax_id = doc.tax_id || "";
      merchant.trade_register_number = doc.trade_register_number || "";
      merchant.iban = (doc.iban === null ? "" : doc.iban) || "";
      merchant.domiciliary_bank_number = doc.domiciliary_bank_number || "";
      merchant.cut_off_time = (doc.cut_off_time || "").slice(0, 5); // HH:MM

      merchant.activation_flag = !!doc.activation_flag;
      merchant.activation_time = isoToLocal(doc.activation_time || "");
      merchant.activation_start_time = isoToLocal(doc.activation_start_time || "");
      merchant.activation_end_time = isoToLocal(doc.activation_end_time || "");

      merchant.start_date = doc.start_date || "";
      merchant.end_date = doc.end_date || "";
      merchant.created_at = doc.created_at || "";
      merchant.updated_at = doc.updated_at || "";

      merchant.description = doc.description || "";

      merchant.status = merchant.activation_flag ? "Active" : "Inactive";

      Object.keys(editFields).forEach(k => editFields[k] = false);
    };

    const fetchByName = async (name) => {
      loading.value = true;
      try {
        const resp = await fetch(`${API_BASE}/v1/merchants/${encodeURIComponent(name)}`);
        if (!resp.ok) throw new Error("Merchant not found");
        const data = await resp.json();
        const doc = data?.merchant || null;
        if (!doc) throw new Error("Merchant not found");
        mapDocToView(doc);
      } finally {
        loading.value = false;
      }
    };

const fetchById = async (id) => {
  loading.value = true;
  try {
    // List all merchant names
    const listResp = await fetch(`${API_BASE}/v1/merchants`);
    if (!listResp.ok) throw new Error("Failed to list merchants");
    const names = (await listResp.json()).merchants || [];

    // Fetch each merchant by name and match on merchant_id
    for (const nm of names) {
      const r = await fetch(`${API_BASE}/v1/merchants/${encodeURIComponent(nm)}`);
      if (!r.ok) continue;
      const d = await r.json();
      if (d?.merchant?.merchant_id === id) {
        mapDocToView(d.merchant);
        loading.value = false;
        return;
      }
    }
    throw new Error("Merchant not found");
  } catch (e) {
    loading.value = false;
    throw e;
  }
};

    const loadFromRoute = async () => {
      try {
        const ident = routeIdentifier.value;
        if (!ident) throw new Error("No merchant specified.");
        if (/^acc_[a-f0-9]+$/i.test(ident)) {
          await fetchById(ident);
        } else {
          await fetchByName(ident);
        }
      } catch (e) {
        loading.value = false;
        originalDoc.value = null;
      }
    };

    const saveUpdates = async () => {
      if (!originalDoc.value) return;
      saving.value = true;
      try {
        const details = {
          merchant_code: merchant.merchant_code || undefined,
          url: merchant.url || undefined,
          language_code: merchant.language_code || undefined,
          time_zone_id: merchant.time_zone_id || undefined,

          legal_name: merchant.legal_name || undefined,
          acceptor_name: merchant.acceptor_name || undefined,
          acceptor_category_code: merchant.acceptor_category_code || undefined,

          country_code: merchant.country_code || undefined,
          country_sub_division_code: merchant.country_sub_division_code || undefined,
          home_country_code: merchant.home_country_code || undefined,
          region_id: merchant.region_id || undefined,
          city: merchant.city || undefined,
          postal_code: merchant.postal_code || undefined,
          street: merchant.street || undefined,
          city_category_code: merchant.city_category_code || undefined,

          business_service_phone_number: merchant.business_service_phone_number || undefined,
          customer_service_phone_number: merchant.customer_service_phone_number || undefined,
          additional_contact_information: merchant.additional_contact_information || undefined,

          currency_code: merchant.currency_code || undefined,
          tax_id: merchant.tax_id || undefined,
          trade_register_number: merchant.trade_register_number || undefined,
          iban: merchant.iban === "" ? null : (merchant.iban || undefined),
          domiciliary_bank_number: merchant.domiciliary_bank_number || undefined,
          cut_off_time: toHHMMSS(merchant.cut_off_time) || undefined,

          activation_flag: !!merchant.activation_flag,
          activation_time: merchant.activation_time ? new Date(merchant.activation_time).toISOString() : undefined,
          activation_start_time: merchant.activation_start_time ? new Date(merchant.activation_start_time).toISOString() : undefined,
          activation_end_time: merchant.activation_end_time ? new Date(merchant.activation_end_time).toISOString() : null,

          description: merchant.description || undefined,
        };

        const payload = {
          merchant_name: originalDoc.value.merchant_name,
          deep_scan: false,
          details,
          start_date: merchant.start_date || undefined,
          end_date: merchant.end_date || undefined,
        };
        const resp = await fetch(`${API_BASE}/v1/onboard`, {
          method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok) throw new Error(data?.detail || "Failed to update merchant");
        if (data?.task_id) {
          await waitForTask(data.task_id);
        } else {
          await loadFromRoute();
        }
        Object.keys(editFields).forEach(k => editFields[k] = false);
        toast.success("Merchant updated successfully", { autoClose: 3000, position: "top-right" });
      } catch (e) {
        toast.error(String(e?.message || e || "Update failed"), { autoClose: 4000, position: "top-right" });
      } finally {
        saving.value = false;
      }
    };

    const waitForTask = async (taskId) => {
      for (let i = 0; i < 40; i++) {
        try {
          const r = await fetch(`${API_BASE}/v1/tasks/${taskId}`);
          const t = await r.json();
          if (t.status === "done") {
            if (t.result?.merchant) mapDocToView(t.result.merchant);
            return;
          }
          if (t.status === "error") throw new Error(t.error || "Update task failed");
        } catch {}
        await new Promise(res => setTimeout(res, 1200));
      }
    };

    // Navigation / actions / modals
    const openSocialUpdates = () => {
      router.push({
        path: `/merchants/${merchant.id || originalDoc.value?.merchant_id}/social-updates`,
        query: { merchantName: merchant.name, merchantId: merchant.id },
      });
    };
    const openEmailBox = () => router.push("/mailbox");
    const openExplore = () => router.push({ path: "/explore", query: { merchant: merchant.name } });

    const confirmPermanentBan = () => {
      actionsTaken.value.push("Permanent Ban");
      showPermanentBanModal.value = false;
      toast.error(`${merchant.name} has been permanently banned!`, { autoClose: 5000, position: "top-right" });
    };
    const confirmShadowBan = () => {
      actionsTaken.value.push("Shadow Ban");
      showShadowBanModal.value = false;
      toast.warning(`${merchant.name} has been shadow banned!`, { autoClose: 5000, position: "top-right" });
    };
    const confirmRestriction = () => {
      if (!restrictionData.reason.trim()) {
        toast.error("Please provide a reason for the restriction.", { autoClose: 3000, position: "top-right" });
        return;
      }
      actionsTaken.value.push("Restricted");
      showRestrictModal.value = false;
      toast.success(`Restrictions applied on ${merchant.name}!`, { autoClose: 4000, position: "top-right" });
    };

    const addCategoryCode = () => {
      const code = selectedCategoryCode.value;
      if (!code) return;
      if (!restrictionData.restrictedCategoryCodes.includes(code)) {
        restrictionData.restrictedCategoryCodes.push(code);
      }
      selectedCategoryCode.value = "";
    };
    const removeCategoryCode = (code) => {
      const ix = restrictionData.restrictedCategoryCodes.indexOf(code);
      if (ix >= 0) restrictionData.restrictedCategoryCodes.splice(ix, 1);
    };

    const openPermanentBanModal = () => (showPermanentBanModal.value = true);
    const closePermanentBanModal = () => (showPermanentBanModal.value = false);
    const openShadowBanModal = () => (showShadowBanModal.value = true);
    const closeShadowBanModal = () => (showShadowBanModal.value = false);
    const openRestrictModal = () => {
      showRestrictModal.value = true;
      restrictionData.dailyTransactionLimit = null;
      restrictionData.monthlyTransactionLimit = null;
      restrictionData.maxTransactionAmount = null;
      restrictionData.restrictedCategoryCodes = [];
      restrictionData.allowedCountries = "";
      restrictionData.requireAdditionalVerification = false;
      restrictionData.blockInternationalTransactions = false;
      restrictionData.reason = "";
      restrictionData.startDate = new Date().toISOString().split("T")[0];
      restrictionData.endDate = "";
      selectedCategoryCode.value = "";
    };
    const closeRestrictModal = () => (showRestrictModal.value = false);

    const goBack = () => router.push("/dashboard");

    onMounted(loadFromRoute);
    watch(() => route.fullPath, loadFromRoute);

    return {
      routeIdentifier,
      merchant, originalDoc, editFields,
      loading, saving, hasChanges, isActive, fullAddress,
      actionsTaken,
      showPermanentBanModal, showShadowBanModal, showRestrictModal,
      openPermanentBanModal, closePermanentBanModal,
      openShadowBanModal, closeShadowBanModal,
      openRestrictModal, closeRestrictModal,
      confirmPermanentBan, confirmShadowBan, confirmRestriction,
      selectedCategoryCode, restrictionData,
      addCategoryCode, removeCategoryCode,
      formatDate,
      saveUpdates, goBack, openEmailBox, openSocialUpdates, openExplore,
      autoRefresh, refreshSec,
      mccLabel,
    };
  },
};
</script>


<style scoped>
/* Layout and overall styles */
.merchant-detail-container {
  margin: 0 auto;
  padding: 20px;
  font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
  background-color: #f5f5f5;
  min-height: 100vh;
}

.layout {
  display: grid;
  grid-template-columns: 320px 1fr;
  gap: 20px;
}

/* Breadcrumb */
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
.breadcrumb-item { color: #6b7280; text-decoration: none; transition: color 0.2s ease; }
.breadcrumb-item:hover { color: #008080; }
.breadcrumb-item.current { color: #008080; font-weight: 500; }
.breadcrumb-separator { color: #d1d5db; user-select: none; }

/* Side panel */
.side-panel {
  background: #ffffff;
  border-radius: 12px;
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
  border: 1px solid #e5e7eb;
  padding: 18px;
  height: fit-content;
  position: sticky;
  top: 20px;
}

.merchant-header {
  background: linear-gradient(135deg, #008080, #20b2aa);
  color: white;
  padding: 16px;
  border-radius: 10px;
  box-shadow: 0 4px 12px rgba(0, 128, 128, 0.2);
  margin-bottom: 14px;
}
.title-row { display: flex; align-items: center; gap: 10px; }
.merchant-title { font-size: 18px; font-weight: 600; margin: 0; }
.merchant-id {
  margin-top: 6px;
  background: rgba(255, 255, 255, 0.2);
  padding: 4px 10px;
  border-radius: 12px;
  font-size: 0.85em; font-weight: 500; display: inline-block;
}
.status-dot { width: 10px; height: 10px; border-radius: 50%; border: 1px solid rgba(255,255,255,0.7); }
.status-dot.active { background: #34d399; }
.status-dot.inactive { background: #ef4444; }

.summary-list { margin-top: 12px; display: grid; gap: 8px; }
.summary-item { display: flex; justify-content: space-between; align-items: center; }
.summary-item .label { color: #6b7280; font-size: 12px; }
.summary-item .value { color: #111827; font-weight: 600; font-size: 13px; }

.panel-section { margin-top: 16px; }
.panel-actions { display: grid; gap: 10px; margin-top: 10px; }
.notification {
  background: #f0fdfa; color: #008080; border: 1px solid #14b8a6; padding: 10px 12px; border-radius: 6px;
  font-size: 14px; font-weight: 600; text-align: center; box-shadow: 0 2px 4px rgba(20, 184, 166, 0.1); cursor: pointer;
}
.notification:hover { background: #008080; color: #f0fdfa; }

.action-center-dropdown { display: flex; flex-direction: column; gap: 8px; }
.action-center-dropdown button {
  padding: 10px 16px; border: 1px solid #14b8a6; border-radius: 6px; background: #ffffff; color: #008080;
  font: 13px "Segoe UI", sans-serif; font-weight: 500; cursor: pointer; transition: all 0.2s ease; width: 100%;
}
.action-center-dropdown button:hover {
  background: #008080; color: #ffffff; transform: translateY(-1px); box-shadow: 0 2px 6px rgba(0, 128, 128, 0.2);
}
.action-center-dropdown button:active { transform: translateY(0); }

.primary-btn{
  background: #008080 !important; color: white !important;
  padding: 1rem !important;
}
.primary-btn:hover {
  background: #006666 !important;
}

.actions-taken { text-align: center; color: #666; font-size: 14px; margin-top: 12px; }
.actions-taken-data { color: #999; font-size: 13px; font-style: italic; }
.actions-taken h1 { text-align: center; color: #008080; font-size: 13px; font-style: italic; }

/* Buttons */
.btn { padding: 10px 14px; border-radius: 6px; font-weight: 600; cursor: pointer; transition: all 0.2s ease; border: 1px solid transparent; }
.btn-primary { background: #008080; color: white; }
.btn-primary:hover { background: #006666; }
.btn-primary:disabled { opacity: 0.6; cursor: not-allowed; }
.btn-secondary { background: white; color: #008080; border-color: #14b8a6; }
.btn-secondary:hover { background: #f0fdfa; }

/* Main content */
.merchant-detail { display: grid; gap: 20px; }

.card {
  background: white; border-radius: 12px; box-shadow: 0 3px 12px rgba(0, 0, 0, 0.08);
  border: 1px solid #e5e7eb; overflow: hidden;
}
.card-header { padding: 16px 18px; background: #f8fafc; border-bottom: 1px solid #e5e7eb; }
.card-header h3 { margin: 0; color: #008080; font-size: 16px; font-weight: 600; }
.card-body { padding: 16px 18px; display: grid; gap: 12px; }
.card-footer { padding: 12px 18px; background: #fafbfc; border-top: 1px solid #e5e7eb; }
.meta-footer { display: flex; gap: 24px; color: #374151; font-size: 13px; }
.grid-2 { grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }

/* Finance badges */
.finance-badges { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 8px; margin-bottom: 6px; }
.finance-badges div { font-size: 13px; color: #374151; }
.finance-badges strong { color: #111827; }

/* FieldRow styles */
.field-row {
  display: grid; grid-template-columns: 200px 1fr auto; align-items: center; gap: 12px;
}
.field-label { color: #374151; font-weight: 600; font-size: 13px; }
.field-value { color: #111827; font-size: 14px; }
.edit-btn {
  background: #f0fdfa; color: #008080; border: 1px solid #14b8a6;
  padding: 6px 10px; border-radius: 6px; cursor: pointer; font-size: 12px;
}
.edit-btn:hover { background: #008080; color: #f0fdfa; }
.edit-input { display: grid; grid-template-columns: 1fr; gap: 8px; margin-top: 6px; }

.form-input, .form-textarea, select.form-input {
  width: 100%; padding: 8px 10px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 14px; transition: all 0.2s ease; box-sizing: border-box;
}
.form-textarea { resize: vertical; min-height: 80px; }

/* Description row */
.desc-row { display: grid; grid-template-columns: 200px 1fr auto; align-items: start; gap: 12px; }
.desc-label { color: #374151; font-weight: 600; font-size: 13px; }
.desc-content { display: grid; gap: 8px; }
.desc-text { color: #111827; font-size: 14px; }
.desc-edit { }

/* Display rows */
.row-display { margin: 0; font-size: 14px; color: #374151; display: flex; gap: 8px; }
.row-display strong { color: #111827; }

/* Modal styles */
.modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0, 0, 0, 0.6); display: flex; justify-content: center; align-items: center; z-index: 1000; backdrop-filter: blur(4px); }
.modal-content { background: white; border-radius: 12px; box-shadow: 0 20px 60px rgba(0,0,0,0.3); max-width: 500px; width: 90%; max-height: 90vh; overflow-y: auto; animation: modalSlideIn 0.3s ease; }
.modal-content.large { max-width: 800px; }
@keyframes modalSlideIn { from { opacity: 0; transform: translateY(-50px) scale(0.9); } to { opacity: 1; transform: translateY(0) scale(1); } }
.modal-header { padding: 20px 24px; border-bottom: 1px solid #e5e7eb; display: flex; justify-content: space-between; align-items: center; background: linear-gradient(135deg, #f8fafc, #f1f5f9); }
.modal-header h3 { margin: 0; color: #008080; font-size: 18px; font-weight: 600; }
.close-btn { background: none; border: none; font-size: 24px; color: #9ca3af; cursor: pointer; padding: 0; width: 30px; height: 30px; display: flex; align-items: center; justify-content: center; border-radius: 50%; transition: all 0.2s ease; }
.close-btn:hover { background: #f3f4f6; color: #374151; }
.modal-body { padding: 24px; }
.warning-icon { font-size: 48px; text-align: center; margin-bottom: 16px; }
.warning-text { font-size: 16px; text-align: center; margin-bottom: 12px; color: #374151; }
.warning-subtext { font-size: 14px; text-align: center; color: #6b7280; margin-bottom: 0; }
.modal-footer { padding: 16px 24px; border-top: 1px solid #e5e7eb; display: flex; justify-content: flex-end; gap: 12px; background: #fafbfc; }

/* Buttons in modal */
.btn-cancel { padding: 10px 20px; border: 1px solid #d1d5db; border-radius: 6px; background: white; color: #374151; font-weight: 500; cursor: pointer; transition: all 0.2s ease; }
.btn-cancel:hover { background: #f9fafb; border-color: #9ca3af; }
.btn-danger { padding: 10px 20px; border: none; border-radius: 6px; background: #dc2626; color: white; font-weight: 500; cursor: pointer; transition: all 0.2s ease; }
.btn-danger:hover { background: #b91c1c; transform: translateY(-1px); }
.btn-warning { padding: 10px 20px; border: none; border-radius: 6px; background: #f59e0b; color: white; font-weight: 500; cursor: pointer; transition: all 0.2s ease; }
.btn-warning:hover { background: #d97706; transform: translateY(-1px); }
.btn-primary { padding: 10px 20px; border: none; border-radius: 6px; background: #008080; color: white; font-weight: 500; cursor: pointer; transition: all 0.2s ease; }
.btn-primary:disabled { opacity: 0.6; cursor: not-allowed; }
.btn-primary:hover { background: #006666; transform: translateY(-1px); }

/* Loading and error */
.loading, .error {
  text-align: center; padding: 40px; color: #666; background: white; border-radius: 12px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);
}

@media (max-width: 1024px) {
  .layout { grid-template-columns: 1fr; }
  .side-panel { position: static; }
}
@media (max-width: 768px) {
  .merchant-detail-container { padding: 15px; }
  .card-body { padding: 14px; }
  .card-header { padding: 14px; }
  .field-row, .desc-row { grid-template-columns: 1fr; }
}
@media (max-width: 480px) {
  .merchant-detail-container { padding: 10px; }
  .card-body { padding: 12px; }
  .card-header { padding: 12px; }
}
</style>