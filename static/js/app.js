const { createApp } = Vue;

createApp({
  data() {
    return {
      me: "",
      items: [],
      current: null,
      policy: null,
      policyLocked: false,
      policyForm: { policy: "drop", note: "" },
      cleanPolicy: "drop",
      fixForms: {},
      busy: false,
      msg: "",
      policyMsg: "",
    };
  },
  computed: {
    overview() {
      const sum = (k) =>
        this.items.reduce((a, p) => a + (p.counts ? p.counts[k] : 0), 0);
      return {
        pending: sum("pending_clean"),
        dropped: sum("dropped"),
        quarantined: sum("quarantined"),
      };
    },
    policyName() {
      return this.policy ? this.policy.name : "未设置";
    },
  },
  async mounted() {
    const me = await fetch("/api/me");
    if (!me.ok) {
      location.href = "/login";
      return;
    }
    this.me = (await me.json()).user;
    await Promise.all([this.refresh(), this.loadPolicy()]);
  },
  methods: {
    tagClass(status) {
      if (status === "已落库" || status === "已清洗待加载") return "ok";
      if (["已抽取待清洗", "隔离待补齐", "清洗中"].includes(status)) return "warn";
      return "";
    },
    evtClass(kind) {
      if (["清洗争用失败", "清洗口径写歪了", "清洗未过关"].includes(kind))
        return "bad";
      if (["清洗通过", "落库", "隔离补齐"].includes(kind)) return "ok";
      return "";
    },
    async refresh() {
      const res = await fetch("/api/pipelines");
      if (!res.ok) return;
      this.items = (await res.json()).items || [];
    },
    async loadPolicy() {
      const res = await fetch("/api/policy");
      if (!res.ok) return;
      const data = await res.json();
      this.policy = data.policy;
      this.policyLocked = data.locked;
      if (this.policy) {
        this.cleanPolicy = this.policy.policy;
        this.policyForm.policy = this.policy.policy;
        this.policyForm.note = this.policy.note || "";
      }
    },
    async open(id) {
      const res = await fetch(`/api/pipelines/${id}`);
      if (!res.ok) return;
      this.current = await res.json();
      const forms = {};
      for (const r of this.current.quarantine || []) {
        forms[r.id] = { title: "", amount: "" };
      }
      this.fixForms = forms;
    },
    async select(id) {
      this.msg = "";
      await this.open(id);
    },
    async afterAction() {
      await this.refresh();
      if (this.current) await this.open(this.current.id);
    },
    async extract() {
      if (!this.current) return;
      this.busy = true;
      let msg = "";
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/extract`, {
          method: "POST",
        });
        const data = await res.json();
        msg = res.ok
          ? `抽取完成，${data.counts.pending_clean} 行待清洗`
          : data.error || "抽取被闸口拦下";
      } finally {
        this.busy = false;
      }
      await this.afterAction();
      this.msg = msg;
    },
    async clean() {
      if (!this.current) return;
      this.busy = true;
      let msg = "";
      const mode = this.current.status === "隔离待补齐" ? "reclean" : "clean";
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/clean`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ policy: this.cleanPolicy, mode }),
        });
        const data = await res.json();
        if (res.ok) {
          msg =
            data.status === "已清洗待加载"
              ? "清洗过关，可落库"
              : data.status === "隔离待补齐"
              ? `清洗完毕，${data.counts.quarantined} 行进隔离筐，补齐前禁止落库`
              : "清洗完成";
        } else {
          msg = data.error || "清洗被闸口拦下";
        }
      } finally {
        this.busy = false;
      }
      await this.afterAction();
      this.msg = msg;
    },
    async load() {
      if (!this.current) return;
      this.busy = true;
      let msg = "";
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/load`, {
          method: "POST",
        });
        const data = await res.json();
        msg = res.ok ? "落库完成" : data.error || "落库被闸口拦下";
      } finally {
        this.busy = false;
      }
      await this.afterAction();
      this.msg = msg;
    },
    async savePolicy() {
      this.busy = true;
      this.policyMsg = "";
      try {
        const res = await fetch("/api/policy", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(this.policyForm),
        });
        const data = await res.json();
        if (!res.ok) {
          this.policyMsg = data.error || "口径被闸口拦下";
        } else {
          this.policy = data.policy;
          this.policyLocked = data.locked;
          this.cleanPolicy = data.policy.policy;
          this.policyMsg = "口径已写进说明，全天不得改弦";
        }
      } finally {
        this.busy = false;
      }
    },
    async fixRow(r) {
      const f = this.fixForms[r.id] || { title: "", amount: "" };
      this.busy = true;
      let msg = "";
      try {
        const res = await fetch(
          `/api/pipelines/${this.current.id}/rows/${r.id}/fix`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ title: f.title, amount: f.amount }),
          }
        );
        const data = await res.json();
        msg = res.ok ? `第 ${r.row_no} 行已补齐` : data.error || "补齐被闸口拦下";
      } finally {
        this.busy = false;
      }
      await this.afterAction();
      this.msg = msg;
    },
    async logout() {
      await fetch("/api/logout", { method: "POST" });
      location.href = "/login";
    },
  },
}).mount("#app");
