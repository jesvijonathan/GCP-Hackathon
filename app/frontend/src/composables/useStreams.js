import { ref, watch } from 'vue';
import { toast } from 'vue3-toastify';

export function useStreams(apiGet, merchantKeyRef, opts) {
  const tweets = ref([]);
  const tweetsLoading = ref(false);
  const tweetsError = ref('');

  const redditPosts = ref([]);
  const redditLoading = ref(false);
  const redditError = ref('');

  const wlTxns = ref([]);
  const wlLoading = ref(false);
  const wlError = ref('');

  const streamWindow = ref('7d');
  const streamOrder = ref('desc');
  const streamLimit = ref(5000);
  const allowFuture = ref(false);
  const sinceStr = ref('');
  const untilStr = ref('');
  const nowIso = ref('');
  const unit = ref('hour');

  let abort = null;
  let inFlight = false;

  async function fetchStreams() {
    if (!merchantKeyRef.value || inFlight) return;
    if (abort) abort.abort();
    abort = new AbortController();

    tweetsLoading.value = redditLoading.value = wlLoading.value = true;
    tweetsError.value = redditError.value = wlError.value = '';
    inFlight = true;
    try {
      const params = {};
      params.streams = 'tweets,reddit,wl';
      params.order = streamOrder.value;
      if (streamWindow.value) params.window = streamWindow.value.trim();
      if (sinceStr.value) params.since = sinceStr.value.trim();
      if (untilStr.value) params.until = untilStr.value.trim();
      if (Number(streamLimit.value) > 0) params.limit = String(Number(streamLimit.value));
      if (allowFuture.value) params.allow_future = 'true';
      if (nowIso.value) params.now = nowIso.value.trim();
      const url = `/v1/${encodeURIComponent(merchantKeyRef.value)}/data`;
      const json = await apiGet(url, params, abort.signal);
      tweets.value = Array.isArray(json?.data?.tweets) ? json.data.tweets : [];
      const redditArr = json?.data?.reddit_posts || json?.data?.reddit || json?.data?.redditPosts || [];
      redditPosts.value = Array.isArray(redditArr) ? redditArr : [];
      const wlArr = json?.data?.wl || json?.data?.wl_transactions || json?.data?.wlTxns || json?.data?.wl_transactions_raw || [];
      wlTxns.value = Array.isArray(wlArr) ? wlArr : [];
    } catch (e) {
      if (String(e?.name) !== 'AbortError') {
        const msg = String(e?.message || e);
        tweetsError.value = redditError.value = wlError.value = msg;
        tweets.value = redditPosts.value = wlTxns.value = [];
        toast.error(msg, { autoClose: 3000 });
      }
    } finally {
      tweetsLoading.value = redditLoading.value = wlLoading.value = false;
      inFlight = false;
    }
  }

  function bindReactiveFetch() {
    watch([
      streamWindow, streamOrder, streamLimit, allowFuture, sinceStr, untilStr
    ], () => fetchStreams());
    watch(merchantKeyRef, () => fetchStreams());
  }

  return {
    // state
    tweets, tweetsLoading, tweetsError,
    redditPosts, redditLoading, redditError,
    wlTxns, wlLoading, wlError,
    streamWindow, streamOrder, streamLimit, allowFuture, sinceStr, untilStr, nowIso, unit,
    // actions
    fetchStreams, bindReactiveFetch,
  };
}
