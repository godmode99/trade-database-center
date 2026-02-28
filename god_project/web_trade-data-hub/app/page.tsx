const upcomingEvents = [
  {
    datetimeBkk: "2026-03-02 20:30",
    currency: "USD",
    impact: "High",
    eventName: "Non-Farm Employment Change",
    forecast: "185K",
    previous: "143K",
    actual: "",
  },
  {
    datetimeBkk: "2026-03-03 17:00",
    currency: "EUR",
    impact: "Medium",
    eventName: "CPI Flash Estimate y/y",
    forecast: "2.4%",
    previous: "2.6%",
    actual: "",
  },
  {
    datetimeBkk: "2026-03-04 22:00",
    currency: "USD",
    impact: "High",
    eventName: "ISM Services PMI",
    forecast: "53.1",
    previous: "52.8",
    actual: "",
  },
  {
    datetimeBkk: "2026-03-05 19:15",
    currency: "EUR",
    impact: "High",
    eventName: "ECB Main Refinancing Rate",
    forecast: "3.00%",
    previous: "3.00%",
    actual: "",
  },
  {
    datetimeBkk: "2026-03-06 20:30",
    currency: "USD",
    impact: "High",
    eventName: "Average Hourly Earnings m/m",
    forecast: "0.3%",
    previous: "0.2%",
    actual: "",
  },
];

const impactClass: Record<string, string> = {
  High: "bg-red-100 text-red-700",
  Medium: "bg-amber-100 text-amber-700",
  Low: "bg-emerald-100 text-emerald-700",
};

export default function Home() {
  const totalEvents = upcomingEvents.length;
  const highImpactCount = upcomingEvents.filter((event) => event.impact === "High").length;
  const currencies = new Set(upcomingEvents.map((event) => event.currency)).size;

  return (
    <div className="min-h-screen bg-slate-100 px-4 py-10 text-slate-900 sm:px-8">
      <main className="mx-auto flex w-full max-w-6xl flex-col gap-6">
        <section className="rounded-2xl bg-white p-6 shadow-sm sm:p-8">
          <p className="text-sm font-semibold uppercase tracking-[0.2em] text-indigo-600">
            Trade Database Center
          </p>
          <h1 className="mt-2 text-3xl font-bold tracking-tight sm:text-4xl">Calendar Section</h1>
          <p className="mt-3 max-w-3xl text-slate-600">
            ส่วนนี้แสดงปฏิทินข่าวเศรษฐกิจที่เกี่ยวข้องกับ EUR และ USD เพื่อใช้ประกอบการตัดสินใจเทรด
            และจะเชื่อมกับข้อมูลจริงจาก pipeline ในเฟสถัดไป
          </p>
        </section>

        <section className="grid gap-4 sm:grid-cols-3">
          <article className="rounded-2xl bg-white p-5 shadow-sm">
            <p className="text-sm text-slate-500">Total Events (Week)</p>
            <p className="mt-1 text-3xl font-bold">{totalEvents}</p>
          </article>
          <article className="rounded-2xl bg-white p-5 shadow-sm">
            <p className="text-sm text-slate-500">High Impact</p>
            <p className="mt-1 text-3xl font-bold">{highImpactCount}</p>
          </article>
          <article className="rounded-2xl bg-white p-5 shadow-sm">
            <p className="text-sm text-slate-500">Tracked Currencies</p>
            <p className="mt-1 text-3xl font-bold">{currencies}</p>
          </article>
        </section>

        <section className="overflow-hidden rounded-2xl bg-white shadow-sm">
          <div className="border-b border-slate-200 px-6 py-4">
            <h2 className="text-lg font-semibold">Upcoming Economic Calendar</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-slate-200 text-sm">
              <thead className="bg-slate-50 text-left text-slate-500">
                <tr>
                  <th className="px-6 py-3 font-semibold">DateTime (BKK)</th>
                  <th className="px-6 py-3 font-semibold">Currency</th>
                  <th className="px-6 py-3 font-semibold">Impact</th>
                  <th className="px-6 py-3 font-semibold">Event</th>
                  <th className="px-6 py-3 font-semibold">Forecast</th>
                  <th className="px-6 py-3 font-semibold">Previous</th>
                  <th className="px-6 py-3 font-semibold">Actual</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 bg-white">
                {upcomingEvents.map((event) => (
                  <tr key={`${event.datetimeBkk}-${event.eventName}`}>
                    <td className="whitespace-nowrap px-6 py-4 font-medium text-slate-700">{event.datetimeBkk}</td>
                    <td className="px-6 py-4">{event.currency}</td>
                    <td className="px-6 py-4">
                      <span
                        className={`inline-flex rounded-full px-2.5 py-1 text-xs font-semibold ${impactClass[event.impact]}`}
                      >
                        {event.impact}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-slate-700">{event.eventName}</td>
                    <td className="px-6 py-4">{event.forecast}</td>
                    <td className="px-6 py-4">{event.previous}</td>
                    <td className="px-6 py-4">{event.actual || "Pending"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  );
}
