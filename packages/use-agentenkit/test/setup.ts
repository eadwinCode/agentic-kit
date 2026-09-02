import { GlobalRegistrator } from '@happy-dom/global-registrator';

// The hook is a browser component: it reads window.location, writes history
// and localStorage. happy-dom gives it those without a real browser.
GlobalRegistrator.register({ url: 'http://localhost/' });
