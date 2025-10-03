<template>
		<div class="risk-wrapper">
		<div class="risk-header">
			<h3 class="title">Risk Evaluation</h3>
			<div class="controls">
				<button class="btn" @click="refresh(true)" :disabled="loading || refreshing">Reload</button>
				<button class="btn alt" @click="reparse" :disabled="loading || refreshing || reparseLoading">{{ reparseLoading ? 'Re-Parsing…' : 'Re-Parse' }}</button>
				<label class="chk" title="Overlay raw (unsmoothed) total if available"><input type="checkbox" v-model="showRaw" /> Raw overlay</label>
				<label class="chk" title="Forward mode: anchor at simulated time (or now) & only show windows from that point forward"><input type="checkbox" v-model="forwardFromSim" /> Forward mode</label>
				<span class="mode-hint" v-if="forwardFromSim">Anchor: {{ anchorDisplay }}</span>
				<span class="mode-hint" v-else>Last 48h (1h)</span>
			</div>
		</div>

			<div v-if="error" class="error-block">{{ error }}</div>
			<div v-else>
				<!-- Debug status (temporary) -->
				<div class="debug-status" style="font-size:11px;color:#036d69;display:flex;gap:10px;flex-wrap:wrap;margin-bottom:4px;">
					<span>rows={{ rows.length }}</span>
					<span>refreshing={{ refreshing }}</span>
					<span>forward={{ forwardFromSim }}</span>
					<span v-if="forwardFromSim">anchorTs={{ anchorDisplay }}</span>
				</div>
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

					<div class="chart-area">
						<canvas ref="chartCanvas" height="140"></canvas>
						<!-- Overlay states -->
						<div v-if="refreshing && initialLoaded" class="overlay-loading">Updating…</div>
						<div v-else-if="refreshing && !initialLoaded" class="overlay-loading">Loading evaluation windows…</div>
						<div v-else-if="!refreshing && !rows.length" class="overlay-loading" style="background:rgba(255,255,255,0.75); color:#036d69;">
							<div style="text-align:center;">
								<div style="font-weight:600;">No data yet</div>
								<div style="font-size:11px;margin-top:4px;">Trigger risk evaluation or seed data to populate.</div>
							</div>
						</div>
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

					<div v-if="!rows.length && initialLoaded && !refreshing" class="empty">No evaluation data (all windows ensured).</div>
					<!-- Removed separate loading block; unified in chart overlay -->
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
			now: { type: String, default: '' },
			loading: { type: Boolean, default: false }
	},
	setup(props) {
		try { console.debug('[MerchantRisk] setup mount merchant=', props.merchant); } catch {}
		// Fixed interval & lookback rules per new requirements
		const FIXED_INTERVAL = '1h'
		const LOOKBACK_HOURS = 48
		const anchorSimTs = ref(null) // forward mode anchor (epoch seconds)
		const anchorDisplay = computed(()=> anchorSimTs.value ? new Date(anchorSimTs.value*1000).toLocaleString([], {hour:'2-digit', minute:'2-digit'}) : '—')
		const rows = ref([])
		const refreshing = ref(false)
		const initialLoaded = ref(false)
		const lastFetchAt = ref(0)
		const error = ref('')
		// Forward mode: anchor at first observed simulated time (or real now) and only move forward (default OFF for immediate data display)
		const forwardFromSim = ref(false)
		const reparseLoading = ref(false)
		// Auto-trigger guard so we don't spam trigger endpoint
		const autoTriggered = ref(false)
		const initializing = ref(false)
		// Raw overlay (unsmoothed total) enabled by default; persisted across sessions
		const showRaw = ref(true)
		const timer = ref(null) // (kept for potential future use, no auto refresh now)
		const chartCanvas = ref(null)
		let chartInstance = null

				async function fetchData(forceEnsure=false) {
					refreshing.value = true
			error.value = ''
			// Failsafe: if initial load exceeds 6s, mark initialLoaded so overlay message changes
			const initialMarkTimer = (!initialLoaded.value) ? setTimeout(()=>{ if(!initialLoaded.value){ initialLoaded.value = true; } }, 6000) : null
			try {
					lastFetchAt.value = Date.now()
			let simOverride = ''
			if (props.now && props.now.trim()) {
				simOverride = props.now.trim()
			} else {
				try { const ls = localStorage.getItem('simNow'); if (ls && ls.trim()) simOverride = ls.trim(); } catch {}
			}
			// --- Simulated time sanitization ---
			let useSim = false
			let simSec = null
			if (simOverride) {
				const parsed = Date.parse(simOverride)
				if(!isNaN(parsed)) {
					simSec = Math.floor(parsed/1000)
					const realNowSec = Math.floor(Date.now()/1000)
					// Allow rewind (past) or slight forward (<=5 min), clamp large future jumps
					if (simSec - realNowSec > 300) {
						console.warn('[MerchantRisk] Ignoring future simulated now >5m ahead:', simOverride)
						useSim = false
						simOverride = ''
					} else {
						useSim = true
					}
				}
			}
			const realUntil = Math.floor(Date.now()/1000)
			let until = useSim ? simSec : realUntil
			// In forward mode with future sim date, keep until = simSec (future) so backend returns empty but UI clarifies state
			// If simulated time is in the past, optionally still limit until to real now
			if(useSim && simSec && simSec > realUntil && !forwardFromSim.value){
				// if user disabled forward mode, clamp to realNow so some data shows
				until = realUntil
			}
			// Establish / maintain forward anchor
			if(forwardFromSim.value){
				if(!anchorSimTs.value){
					// anchor at simulated time if available, else real now
					anchorSimTs.value = useSim && simSec ? simSec : realUntil
				}
			}
			else {
				anchorSimTs.value = null
			}
			let since
			if(forwardFromSim.value && anchorSimTs.value){
				// Only show from anchor forward (do not backfill earlier)
				since = anchorSimTs.value
			} else {
				// Fixed 48h lookback (respect simulation if provided)
				since = (until - LOOKBACK_HOURS*3600)
			}
			// Clamp until so we never query > real now + 5s
			if(until > realUntil + 5) until = realUntil
			const url = new URL(`${API_BASE}/v1/${encodeURIComponent(props.merchant)}/evaluations`)
			url.searchParams.set('interval', FIXED_INTERVAL)
				url.searchParams.set('since', since)
				url.searchParams.set('until', until)
				url.searchParams.set('limit', 1000)
			// Always ensure so new windows are materialized as time advances
			url.searchParams.set('ensure', 'true')
						if (useSim) { url.searchParams.set('now', simOverride) }
				const startedAt = performance.now()
				const res = await fetch(url.toString())
				if(!res.ok) throw new Error(`HTTP ${res.status}`)
				const data = await res.json()
				const dur = Math.round(performance.now() - startedAt)
				try { console.debug('[MerchantRisk] fetched evaluations', { merchant: props.merchant, interval: FIXED_INTERVAL, since, until, count: (data.rows||[]).length, ms: dur, sim: useSim, forwardFrom: forwardFromSim.value, anchor: anchorSimTs.value, url: url.toString() }); } catch {}
				let fetched = Array.isArray(data.rows)? data.rows : []
				// Filter out rows before simulated anchor in forward mode
				if(useSim && forwardFromSim.value && simSec){
					fetched = fetched.filter(r => (r.window_end_ts||0) >= simSec)
				}
				// Defensive interval filter
				const ivMap = { '30m':30, '1h':60, '1d':1440 }
				const expectMinutes = ivMap[FIXED_INTERVAL] || 60
				fetched = fetched.filter(r => !r.interval_minutes || r.interval_minutes === expectMinutes)
				fetched.sort((a,b)=> (a.window_end_ts||0)-(b.window_end_ts||0))
				rows.value = fetched
				if(!rows.value.length) {
					console.warn('[MerchantRisk] empty fetch result, retrying fallback (24h)')
					// Fallback once: try 24h lookback if not forward mode
					if(!forwardFromSim.value){
						const fallbackSince = until - 24*3600
						try {
							const fb = new URL(url.toString())
							fb.searchParams.set('since', fallbackSince)
							const r2 = await fetch(fb.toString())
							if(r2.ok){ const js2 = await r2.json(); if(Array.isArray(js2.rows) && js2.rows.length){ rows.value = js2.rows.sort((a,b)=> (a.window_end_ts||0)-(b.window_end_ts||0)); }
						}
						} catch(e){ console.warn('[MerchantRisk] fallback 24h fetch failed', e) }
					}
					console.warn('[MerchantRisk] No evaluation rows returned. since/until=', since, until, 'forceEnsure=', forceEnsure)
					// Auto-trigger risk evaluation (once) to materialize windows
					if(!autoTriggered.value && props.merchant) {
						try {
							initializing.value = true
							autoTriggered.value = true
							let trigUrl = new URL(`${API_BASE}/v1/${encodeURIComponent(props.merchant)}/risk-eval/trigger`)
							trigUrl.searchParams.set('interval', FIXED_INTERVAL)
							trigUrl.searchParams.set('autoseed','true')
							// No backward backfill: start fresh from sim or now
							trigUrl.searchParams.set('max_backfill_hours','0')
							trigUrl.searchParams.set('priority','4')
							if(useSim && simOverride) trigUrl.searchParams.set('now', simOverride)
							// If forward mode with future sim, supply now so windows anchor there
							await fetch(trigUrl.toString(), { method:'POST' })
							// Retry fetch after short delay (let background ensure run)
							setTimeout(()=> { fetchData(false) }, 1200)
							// Second delayed retry if still empty
							setTimeout(()=> { if(!rows.value.length) fetchData(false) }, 3200)
						} catch(trigErr){
							console.warn('[MerchantRisk] auto trigger failed', trigErr)
						} finally {
							setTimeout(()=> { initializing.value = false }, 4000)
						}
					} else if(forceEnsure) {
						// One manual retry after short delay if first attempt
						setTimeout(()=> { if(!rows.value.length) fetchData(false) }, 1500)
					}
				}
				ensureChartVisible()
			} catch(e) {
				error.value = e.message || String(e)
			} finally {
				refreshing.value = false
				if(!initialLoaded.value) initialLoaded.value = true
				if(initialMarkTimer) clearTimeout(initialMarkTimer)
			}
		}

		function refresh(forceEnsure=false){
			fetchData(forceEnsure)
		}

		async function reparse(){
			if(reparseLoading.value) return
			reparseLoading.value = true
			try {
				// Derive current temporal bounds for targeted recompute
				let simOverride = ''
				try { const ls = localStorage.getItem('simNow'); if(ls) simOverride = ls.trim() } catch {}
				let parsedSim = null
				if(simOverride){ const p = Date.parse(simOverride); if(!isNaN(p)) parsedSim = Math.floor(p/1000) }
				const realNow = Math.floor(Date.now()/1000)
				const until = (forwardFromSim.value && parsedSim) ? parsedSim : (parsedSim || realNow)
				const since = forwardFromSim.value ? (anchorSimTs.value || parsedSim || until) : (until - LOOKBACK_HOURS*3600)
				let trig = new URL(`${API_BASE}/v1/${encodeURIComponent(props.merchant)}/risk-eval/trigger`)
				trig.searchParams.set('interval', FIXED_INTERVAL)
				trig.searchParams.set('priority','3')
				trig.searchParams.set('autoseed','true')
				trig.searchParams.set('max_backfill_hours','') // allow dynamic backfill inside since/until
				trig.searchParams.set('since', since)
				trig.searchParams.set('until', until)
				if(parsedSim) trig.searchParams.set('now', parsedSim)
				await fetch(trig.toString(), { method:'POST' })
				// Give backend a moment to enqueue & compute
				setTimeout(()=> refresh(true), 1200)
			} catch(e){
				console.warn('[MerchantRisk] reparse failed', e)
			} finally {
				setTimeout(()=> { reparseLoading.value = false }, 1500)
			}
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
			if(v>=80) return 'risk-high'
			if(v>=55) return 'risk-med'
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
				// Build timestamp array & rich date-time labels similar to dashboard overview chart
				const ts = rows.value.map(r=> r.window_end_ts).filter(v=> typeof v==='number').sort((a,b)=> a-b)
				// Fallback if window_end_ts missing
				let lastDayKey = null
				const labels = rows.value.map(r=> {
					const d = r.window_end ? new Date(r.window_end) : (r.window_end_ts? new Date(r.window_end_ts*1000): null)
					if(!d) return ''
					const datePart = d.toLocaleDateString([], {month:'short', day:'2-digit'})
					const timePart = d.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'})
					const key = d.getFullYear()+':' + d.getMonth()+':' + d.getDate()
					if(key !== lastDayKey){ lastDayKey = key; return `${datePart} ${timePart}` }
					return timePart
				})
				const totals = rows.value.map(r=> r.scores?.total)
				const rawTotals = rows.value.map(r=> r.scores?.total_raw ?? null)
				const { default: Chart } = await import('chart.js/auto')
				// Threshold color function (match dashboard)
				const colorFor = (v) => {
					if(v==null || !isFinite(v)) return '#9ca3af'
					if(v >= 90) return '#7f1d1d'
					if(v >= 80) return '#dc2626'
					if(v >= 70) return '#ea580c'
					if(v >= 55) return '#f59e0b'
					if(v >= 40) return '#059669'
					return '#6ee7b7'
				}
				const baseDs = {
					label: 'Total Risk',
					data: totals,
					borderWidth:2,
					pointRadius:0,
					spanGaps:true,
					tension:0.3,
					segment:{ borderColor: ctx => colorFor(ctx.p1.parsed.y) },
					borderColor:'#059669'
				}
				const rawDs = {
					label:'Raw Total',
					data: rawTotals,
					borderWidth:1,
					borderDash:[4,4],
					spanGaps:true,
					pointRadius:0,
					tension:0.2,
					borderColor:'#64748b'
				}
				const datasets = showRaw.value ? [baseDs, rawDs] : [baseDs]
				// Risk bands plugin (same thresholds list)
				const RiskBands = {
					id:'riskBands',
					beforeDraw: (chart) => {
						const {ctx, chartArea, scales:{y}} = chart; if(!y) return;
						const bands = [
							{from:90,to:100,color:'rgba(127,29,29,0.10)'},
							{from:80,to:90,color:'rgba(220,38,38,0.08)'},
							{from:70,to:80,color:'rgba(234,88,12,0.07)'},
							{from:55,to:70,color:'rgba(245,158,11,0.06)'},
							{from:40,to:55,color:'rgba(5,150,105,0.05)'},
							{from:0,to:40,color:'rgba(110,231,183,0.04)'}
						]
						ctx.save()
						bands.forEach(b=>{
							if(y.max < b.from || y.min > b.to) return;
							const top = y.getPixelForValue(Math.min(b.to,y.max))
							const bottom = y.getPixelForValue(Math.max(b.from,y.min))
							const h = bottom - top; if(h<=0) return;
							ctx.fillStyle = b.color; ctx.fillRect(chartArea.left, top, chartArea.width, h)
						})
						ctx.restore()
					}
				}
				if(chartInstance){
					chartInstance.data.labels = labels
					chartInstance.data.datasets = datasets
					chartInstance.update('none')
					return
				}
				const ctx = chartCanvas.value.getContext('2d')
				Chart.register(RiskBands)
				chartInstance = new Chart(ctx, {
					type:'line',
					data:{ labels, datasets },
					options:{
						animation:false,
						responsive:true,
						maintainAspectRatio:false,
						scales:{
							y:{ beginAtZero:true, min:0, max:100, ticks:{ stepSize:10 } }
						},
						plugins:{
							legend:{ display: showRaw.value, position:'bottom', labels:{ usePointStyle:true, pointStyle:'line', padding:10 }},
							tooltip:{
								backgroundColor:'rgba(6,95,91,0.92)',
								borderColor:'#14b8a6', borderWidth:1, padding:10, cornerRadius:8,
								callbacks:{
									title:(items)=>{ if(!items.length) return ''; const idx = items[0].dataIndex; const row = rows.value[idx]; const d = row?.window_end ? new Date(row.window_end) : (row?.window_end_ts? new Date(row.window_end_ts*1000): null); return d? d.toLocaleString([], {year:'numeric', month:'short', day:'2-digit', hour:'2-digit', minute:'2-digit'}):''; },
									label:(ctx)=> `${ctx.dataset.label}: ${ctx.formattedValue}`
								}
							}
						}
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
				watch(showRaw, (v)=> { try { localStorage.setItem('mrShowRaw', JSON.stringify(!!v)); } catch(_) {} buildChart() })
				watch(forwardFromSim, (nv, ov)=>{
					if(nv){
						// establish new anchor on enable
						anchorSimTs.value = null // will be set on next fetch
					} else {
						anchorSimTs.value = null
					}
					refresh(true)
				})
				// React when merchant changes (parent selection switch)
				watch(()=> props.merchant, (nv, ov)=> {
					if(!nv || nv === ov) return;
					// reset state & destroy existing chart to avoid data bleed
					rows.value = [];
					if(chartInstance){ try { chartInstance.destroy(); } catch(_) {} chartInstance = null; }
					initialLoaded.value = false;
					visibilityTries = 0;
					fetchData(true);
				})

        // Auto refresh removed per new requirements

				onMounted(()=>{
				// Load persisted raw overlay preference
				try {
					const stored = localStorage.getItem('mrShowRaw')
					if(stored !== null) showRaw.value = JSON.parse(stored)
				} catch(_) {}
				fetchData(true)
				// Fallback periodic visibility check in early lifecycle
				setTimeout(()=> ensureChartVisible(), 500)
				window.addEventListener('resize', ensureChartVisible)
						// Listen for simulated time change to re-anchor since/until
						try {
							const handler = (e)=>{
								if(!e || !e.detail) return;
								// Force ensure so new windows materialize relative to new simulated now
								fetchData(true);
							};
							window.addEventListener('sim-now-updated', handler);
							// store for removal
							window.__mrSimHandler = handler;
						} catch {}
			})
					onBeforeUnmount(()=>{ clearInterval(timer.value); window.removeEventListener('resize', ensureChartVisible); try { if(window.__mrSimHandler) window.removeEventListener('sim-now-updated', window.__mrSimHandler); } catch {}; if(chartInstance) chartInstance.destroy() })

			return { rows, refreshing, initialLoaded, error, showRaw, forwardFromSim, autoTriggered, initializing, refresh, reparse, reparseLoading, chartCanvas, formatScore, formatPct, timeFmt, riskClass, latestTotal, latestConfidence, avgTotal, componentList, anchorDisplay }
	}
}
</script>

<style scoped>
.risk-wrapper { display:flex; flex-direction:column; gap:14px; }
.risk-header { display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }
.risk-header .controls { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
select { padding:4px 6px; border:1px solid #ccc; border-radius:4px; background:#fff; font-size:13px; }
.btn { background:#008080; color:#fff; border:none; padding:6px 12px; border-radius:4px; cursor:pointer; font-size:13px; }
.btn.alt { background:#036d69; }
.btn:disabled { opacity:.4; cursor:not-allowed; }
.chk { font-size:12px; display:flex; align-items:center; gap:4px; }
.mode-hint { font-size:11px; color:#036d69; background:#ecfdf5; padding:3px 6px; border-radius:4px; border:1px solid #a7f3d0; }
.kpi-row { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); gap:10px; }
.kpi { background:#f8fafc; border:1px solid #e5e7eb; border-radius:8px; padding:10px 12px; display:flex; flex-direction:column; gap:4px; }
.kpi .label { font-size:11px; color:#6b7280; text-transform:uppercase; letter-spacing:.5px; }
.kpi .val { font-size:18px; font-weight:600; }
.risk-low { color:#059669; }
.risk-med { color:#f59e0b; }
.risk-high { color:#dc2626; }
.chart-area { position:relative; height:260px; background:#fff; border:1px solid #e5e7eb; border-radius:8px; padding:6px 10px; }
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
@media (max-width:700px){ .chart-area { height:200px; } }
</style>
