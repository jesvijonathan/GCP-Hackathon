<template>
		<div class="risk-wrapper">
		<div class="risk-header">
			<h3 class="title">Risk Evaluation</h3>
			<div class="controls">
				<select v-model="interval" @change="refresh(true)">
					<option value="30m">30m</option>
					<option value="1h">1h</option>
					<option value="1d">1d</option>
				</select>
				<select v-model.number="lookbackHours" @change="refresh(true)">
					<option :value="6">6h</option>
					<option :value="12">12h</option>
					<option :value="24">24h</option>
					<option :value="72">72h</option>
				</select>
				<button class="btn" @click="refresh(true)" :disabled="loading">Reload</button>
				<label class="chk"><input type="checkbox" v-model="auto" /> Auto (30s)</label>
			</div>
		</div>

			<div v-if="error" class="error-block">{{ error }}</div>
			<div v-else>
				<div class="kpi-row" v-if="rows.length">
				<div class="kpi">
					<span class="label">Latest Total</span>
					<span class="val" :class="riskClass(latestTotal)">{{ formatScore(latestTotal) }}</span>
				</div>
				<div class="kpi">
					<span class="label">Confidence</span>
					<span class="val">{{ formatPct(latestConfidence) }}</span>
				</div>
				<div class="kpi">
					<span class="label">Avg Total</span>
					<span class="val">{{ formatScore(avgTotal) }}</span>
				</div>
				<div class="kpi">
					<span class="label">Windows</span>
					<span class="val">{{ rows.length }}</span>
				</div>
			</div>

					<div v-if="rows.length" class="chart-area">
						<canvas ref="chartCanvas" height="140"></canvas>
						<div v-if="refreshing && initialLoaded" class="overlay-loading">Updating…</div>
					</div>

			<div v-if="rows.length" class="component-cards">
				<div v-for="c in componentList" :key="c.key" class="component-card">
					<h4>{{ c.label }}</h4>
					<div class="score" :class="riskClass(c.latest)">{{ formatScore(c.latest) }}</div>
					<div class="mini-bar">
						<div class="bar-bg">
							<div class="bar-fill" :style="{width: (c.latest||0) + '%'}"></div>
						</div>
					</div>
					<div class="meta-line">Avg: {{ formatScore(c.avg) }}</div>
				</div>
			</div>

					<div class="table-wrap" v-if="rows.length">
				<table class="risk-table">
					<thead>
						<tr>
							<th>End</th>
							<th>Total</th>
							<th>WL</th>
							<th>Market</th>
							<th>Sentiment</th>
							<th>Volume</th>
							<th>Inc.Bump</th>
							<th>Conf</th>
							<th>Twt</th>
							<th>Red</th>
							<th>News</th>
							<th>Rev</th>
							<th>WL Tx</th>
							<th>StockPx</th>
						</tr>
					</thead>
					<tbody>
						<tr v-for="r in rows" :key="r.window_end_ts">
							<td>{{ timeFmt(r.window_end) }}</td>
							<td :class="riskClass(r.scores?.total)">{{ formatScore(r.scores?.total) }}</td>
							<td>{{ formatScore(r.scores?.wl) }}</td>
							<td>{{ formatScore(r.scores?.market) }}</td>
							<td>{{ formatScore(r.scores?.sentiment) }}</td>
							<td>{{ formatScore(r.scores?.volume) }}</td>
							<td>{{ formatScore(r.scores?.incident_bump) }}</td>
							<td>{{ formatPct(r.confidence) }}</td>
							<td>{{ r.counts?.tweets || 0 }}</td>
							<td>{{ r.counts?.reddit || 0 }}</td>
							<td>{{ r.counts?.news || 0 }}</td>
							<td>{{ r.counts?.reviews || 0 }}</td>
							<td>{{ r.counts?.wl || 0 }}</td>
							<td>{{ r.counts?.stock_prices || 0 }}</td>
						</tr>
					</tbody>
				</table>
			</div>

					<div v-if="!rows.length && initialLoaded && !refreshing" class="empty">No evaluation data.</div>
					<div v-if="!initialLoaded && refreshing" class="loading">Loading...</div>
		</div>
	</div>
</template>

<script>
import { ref, onMounted, onBeforeUnmount, watch, computed } from 'vue'

const API_BASE = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000'

export default {
	name: 'MerchantRisk',
	props: {
		merchant: { type: String, required: true },
			now: { type: String, default: '' }
	},
	setup(props) {
		const interval = ref('30m')
		const lookbackHours = ref(24)
		const rows = ref([])
		const refreshing = ref(false)
		const initialLoaded = ref(false)
		const error = ref('')
		const auto = ref(false)
		const timer = ref(null)
		const chartCanvas = ref(null)
		let chartInstance = null

				async function fetchData(forceEnsure=false) {
					refreshing.value = true
			error.value = ''
			try {
			const until = props.now && props.now.trim() ? Math.floor(new Date(props.now).getTime()/1000) : Math.floor(Date.now()/1000)
				const since = until - lookbackHours.value*3600
						const url = new URL(`${API_BASE}/v1/${encodeURIComponent(props.merchant)}/evaluations`)
				url.searchParams.set('interval', interval.value)
				url.searchParams.set('since', since)
				url.searchParams.set('until', until)
				url.searchParams.set('limit', 1000)
			// Always ensure so new windows are materialized as time advances
			url.searchParams.set('ensure', 'true')
						if (props.now && props.now.trim()) {
							url.searchParams.set('now', props.now.trim())
						}
				const res = await fetch(url.toString())
				if(!res.ok) throw new Error(`HTTP ${res.status}`)
				const data = await res.json()
				rows.value = (data.rows||[]).sort((a,b)=> (a.window_end_ts||0)-(b.window_end_ts||0))
					// Defer chart build until visible
					ensureChartVisible()
			} catch(e) {
				error.value = e.message || String(e)
					} finally {
						refreshing.value = false
						if(!initialLoaded.value) initialLoaded.value = true
					}
		}

		function refresh(forceEnsure=false){
			fetchData(forceEnsure)
		}

		function formatScore(v){
			if(v===null||v===undefined) return '—'
			return Number(v).toFixed(1)
		}
		function formatPct(v){
			if(v===null||v===undefined) return '—'
			return (v*100).toFixed(0)+'%'
		}
		function timeFmt(iso){
			if(!iso) return ''
			try { const d=new Date(iso); return d.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'}) } catch { return iso }
		}
		function riskClass(v){
			if(v==null) return ''
			if(v>=75) return 'risk-high'
			if(v>=50) return 'risk-med'
			return 'risk-low'
		}

		const latest = computed(()=> rows.value[rows.value.length-1])
		const latestTotal = computed(()=> latest.value?.scores?.total ?? null)
		const latestConfidence = computed(()=> latest.value?.confidence ?? null)
		const avgTotal = computed(()=> {
			const vals=rows.value.map(r=> r.scores?.total).filter(v=> typeof v==='number')
			if(!vals.length) return null
			return vals.reduce((a,b)=>a+b,0)/vals.length
		})
		const componentList = computed(()=>{
			const keys=[
				{key:'wl', label:'WL'},
				{key:'market', label:'Market'},
				{key:'sentiment', label:'Sentiment'},
				{key:'volume', label:'Volume'},
				{key:'incident_bump', label:'Incident'}
			]
			return keys.map(k=>{
				const latestVal = latest.value?.scores?.[k.key]
				const arr = rows.value.map(r=> r.scores?.[k.key]).filter(v=> typeof v==='number')
				const avg = arr.length? arr.reduce((a,b)=>a+b,0)/arr.length : null
				return { ...k, latest: latestVal, avg }
			})
		})

				async function buildChart(){
				if(!chartCanvas.value) return
				const points = rows.value.map(r=> r.scores?.total)
				const labels = rows.value.map(r=> timeFmt(r.window_end))
				const { default: Chart } = await import('chart.js/auto')
				if(chartInstance){
					chartInstance.data.labels = labels
					if(chartInstance.data.datasets[0]){
						chartInstance.data.datasets[0].data = points
					}
					chartInstance.update('none') // no animation
					return
				}
				const ctx= chartCanvas.value.getContext('2d')
				chartInstance = new Chart(ctx, {
					type:'line',
					data:{ labels, datasets:[{ label:'Total Risk', data:points, borderColor:'#008080', backgroundColor:'rgba(0,128,128,0.15)', tension:0.25, spanGaps:true, pointRadius:0 }] },
					options:{
						animation:false,
							responsive:true,
							maintainAspectRatio:false,
							scales:{ y:{ beginAtZero:true, suggestedMax:100 }},
							plugins:{ legend:{ display:false } }
					}
				})
			}

				// Retry logic to build chart only after the canvas is actually laid out with a width > 0
				let visibilityTries = 0
				function ensureChartVisible(){
					if(!rows.value.length) return
					if(!chartCanvas.value){
						requestAnimationFrame(()=> ensureChartVisible())
						return
					}
					const el = chartCanvas.value
					const hasSize = el.offsetWidth > 0 && el.offsetHeight >= 0
					if(hasSize){
						buildChart()
					} else if(visibilityTries < 40){ // ~8s worst case at 200ms
						visibilityTries++
						setTimeout(ensureChartVisible, 200)
					}
				}

				// Re-run visibility check when rows change (e.g., first fetch) or when interval/lookback changes
				watch(rows, ()=> { visibilityTries = 0; ensureChartVisible() })
				watch(()=> interval.value, ()=> { visibilityTries = 0; ensureChartVisible() })
				watch(()=> lookbackHours.value, ()=> { visibilityTries = 0; ensureChartVisible() })

		function setupAuto(){
			clearInterval(timer.value)
			if(auto.value){
				timer.value = setInterval(()=> fetchData(false), 30000)
			}
		}

		watch(auto, setupAuto)

				onMounted(()=>{
				fetchData(true)
				// Fallback periodic visibility check in early lifecycle
				setTimeout(()=> ensureChartVisible(), 500)
				setupAuto()
				window.addEventListener('resize', ensureChartVisible)
			})
			onBeforeUnmount(()=>{ clearInterval(timer.value); window.removeEventListener('resize', ensureChartVisible); if(chartInstance) chartInstance.destroy() })

				return { interval, lookbackHours, rows, refreshing, initialLoaded, error, auto, refresh, chartCanvas, formatScore, formatPct, timeFmt, riskClass, latestTotal, latestConfidence, avgTotal, componentList }
	}
}
</script>

<style scoped>
.risk-wrapper { display:flex; flex-direction:column; gap:14px; }
.risk-header { display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }
.risk-header .controls { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
select { padding:4px 6px; border:1px solid #ccc; border-radius:4px; background:#fff; font-size:13px; }
.btn { background:#008080; color:#fff; border:none; padding:6px 12px; border-radius:4px; cursor:pointer; font-size:13px; }
.btn:disabled { opacity:.4; cursor:not-allowed; }
.chk { font-size:12px; display:flex; align-items:center; gap:4px; }
.kpi-row { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); gap:10px; }
.kpi { background:#f8fafc; border:1px solid #e5e7eb; border-radius:8px; padding:10px 12px; display:flex; flex-direction:column; gap:4px; }
.kpi .label { font-size:11px; color:#6b7280; text-transform:uppercase; letter-spacing:.5px; }
.kpi .val { font-size:18px; font-weight:600; }
.risk-low { color:#059669; }
.risk-med { color:#d97706; }
.risk-high { color:#dc2626; }
.chart-area { position:relative; height:180px; background:#fff; border:1px solid #e5e7eb; border-radius:8px; padding:6px 10px; }
.component-cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:10px; }
.component-card { background:#ffffff; border:1px solid #e5e7eb; border-radius:8px; padding:10px 12px; display:flex; flex-direction:column; gap:6px; }
.component-card h4 { margin:0; font-size:12px; font-weight:600; letter-spacing:.5px; color:#008080; text-transform:uppercase; }
.component-card .score { font-size:20px; font-weight:600; }
.mini-bar { height:6px; }
.bar-bg { background:#f1f5f9; height:6px; border-radius:3px; overflow:hidden; }
.bar-fill { background:#14b8a6; height:100%; }
.meta-line { font-size:11px; color:#6b7280; }
.table-wrap { max-height:300px; overflow:auto; border:1px solid #e5e7eb; border-radius:8px; }
table.risk-table { width:100%; border-collapse:collapse; font-size:12px; }
.risk-table th { position:sticky; top:0; background:#f0fdfa; border-bottom:1px solid #14b8a6; padding:4px 6px; text-align:left; font-weight:600; font-size:11px; }
.risk-table td { padding:4px 6px; border-bottom:1px solid #f1f5f9; white-space:nowrap; }
.risk-table tbody tr:hover { background:#f9fafb; }
.empty { font-size:13px; color:#6b7280; padding:8px 4px; }
.loading { font-size:13px; color:#374151; }
.overlay-loading { position:absolute; inset:0; background:rgba(255,255,255,0.55); backdrop-filter:blur(2px); display:flex; align-items:center; justify-content:center; font-size:12px; font-weight:600; color:#065f5b; border-radius:6px; pointer-events:none; }
.error-block { background:#fee2e2; color:#991b1b; border:1px solid #dc2626; padding:8px 10px; border-radius:6px; font-size:12px; }
@media (max-width:700px){ .chart-area { height:140px; } }
</style>
