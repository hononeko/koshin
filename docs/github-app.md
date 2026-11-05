# The koshin GitHub App

koshin runs Renovate as a GitHub App rather than with a personal access token. The App gets rate limits that
grow with each installation, one webhook for every repository it's installed on, its own bot identity, and the list of
repositories for the sweeper to scan. Without an App, koshin falls back to `GITHUB_TOKEN`.

[`github-app/manifest.json`](../github-app/manifest.json) registers the App through GitHub's
[manifest flow](https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-from-a-manifest). Its
permissions are the ones [Renovate asks for](https://docs.renovatebot.com/modules/platform/github/#running-as-a-github-app):

| Permission | Access | Why |
| :--- | :--- | :--- |
| Contents | write | branches and commits |
| Pull requests | write | Renovate's PRs |
| Issues | write | the Dependency Dashboard |
| Checks, Commit statuses | write | Renovate's status checks |
| Workflows | write | updates to GitHub Actions workflow files |
| Merge queues | write | adding PRs to a merge queue, where a repo uses one |
| Administration, Dependabot alerts, Metadata | read | branch protection, vulnerability alerts, repository metadata |
| Members (organization) | read | team reviewers |

It subscribes to `push`, `pull_request` and `issues`. GitHub always sends an App `installation` and
`installation_repositories`: when the App is installed on repositories, koshin runs each of them once.

## Register the App

1. Edit `github-app/manifest.json`:
   * `name`: unique on GitHub, e.g. `koshin-<your-org>`.
   * `hook_attributes.url`: the proxy's GitHub webhook, `https://<proxy host>/webhooks/github`.
   * Keep `public: false` unless the App must be installed on other accounts too; then set the allow-list first
     (see [Public App](#public-app-and-allowed-accounts)).
2. Save this as `register.html`, paste `manifest.json` into the text area, and open it in a browser. For an organization,
   post to `https://github.com/organizations/<org>/settings/apps/new?state=koshin` instead:

   ```html
   <form action="https://github.com/settings/apps/new?state=koshin" method="post">
     <textarea name="manifest" rows="30" cols="100"></textarea>
     <button type="submit">Create the GitHub App</button>
   </form>
   ```

3. Create the App. GitHub redirects back to this page with `?code=…&state=koshin` in the address bar.
4. Within an hour, exchange the code for the App's credentials:

   ```sh
   curl -sX POST https://api.github.com/app-manifests/<code>/conversions > app.json
   jq '{id, slug, webhook_secret}' app.json
   jq -r .pem app.json > private-key.pem
   ```

5. Hand them to koshin, then delete `app.json`:

   ```sh
   kubectl -n koshin create secret generic koshin-github-app --from-file=private-key.pem
   kubectl -n koshin create secret generic koshin-proxy \
     --from-literal=GITHUB_WEBHOOK_SECRET="$(jq -r .webhook_secret app.json)" \
     --from-literal=INTERNAL_TRIGGER_TOKEN="$(openssl rand -hex 24)"
   helm upgrade --install koshin oci://ghcr.io/hononeko/charts/koshin -n koshin \
     --set github.app.id="$(jq -r .id app.json)" \
     --set github.app.existingSecret=koshin-github-app \
     --set proxy.existingSecret=koshin-proxy
   ```

   Outside the chart, set `GITHUB_APP_ID` and `GITHUB_APP_PRIVATE_KEY_PATH` on the proxy and the worker, and
   `GITHUB_WEBHOOK_SECRET` on the proxy.
6. Install the App (`https://github.com/apps/<slug>/installations/new`) on the repositories Renovate should manage.

On GitHub Enterprise Server, use `https://<ghes>/settings/apps/new` and
`https://<ghes>/api/v3/app-manifests/<code>/conversions`, and set `GITHUB_API_URL` (`github.apiUrl`) to
`https://<ghes>/api/v3`.

## Public App and allowed accounts

A private App can only be installed on the account that owns it. To install it on another user or organization too
(e.g. your own account and your organization), the App must be **public**, and then anyone who finds it can install it
and point your workers, API budget and registry credentials at their repositories. GitHub can't restrict who installs a
public App, so koshin does:

* `GITHUB_APP_ALLOWED_ACCOUNTS` (chart: `github.app.allowedAccounts`) lists the users and organizations koshin serves.
  Use numeric account IDs, since a login can be renamed and a freed login registered by someone else:
  `gh api users/<login> --jq .id`. Logins work too: they're resolved to IDs at startup and matched by ID from then on
  (unauthenticated, so on GHES in private mode they can't be, and are matched by login: use IDs there).
  Unset, every installation is served (and a warning logged), which is only right for a private App.
* Installations by other accounts get nothing: their webhooks are ignored, triggers and the sweeper skip their
  repositories, and no installation token is minted for them (a job that slips through is dropped, not retried).
* `GITHUB_APP_DISALLOWED_ACTION` (`github.app.disallowedAction`) also acts on them, when they install the App and on a
  sweep:
  * `ignore` (default): leave them installed. Nothing runs, but the account still sees the App as installed.
  * `suspend`: suspend the installation. The account sees it suspended; you can unsuspend it from the App's settings.
  * `uninstall`: remove it. Cleanest, but the account can install it again, and is removed again each time.

Set the allow-list, then make the App public (App settings → Advanced → Make public).

## How koshin uses it

* **Runs:** the worker looks up each repository's installation once (`GET /repos/{owner}/{repo}/installation`) and
  remembers it until GitHub answers 401 or 404 for it. It mints installation tokens per installation and keeps them in
  memory only. A token lasts 60 minutes, so a run only gets one with at least `CLI_TIMEOUT_MINUTES + 5` minutes left:
  runs within about 10 minutes of each other share a token, later ones get a new one. `CLI_TIMEOUT_MINUTES` must
  therefore stay below 55; the worker refuses to start otherwise.
* **Identity:** Renovate commits and opens PRs as `<slug>[bot]` (`RENOVATE_USERNAME`, and `RENOVATE_GIT_AUTHOR` with the
  bot's noreply email), and the proxy ignores that bot's webhooks, so Renovate's own pushes don't trigger runs.
* **Sweeper:** `POST /api/v1/trigger {"discover": true}` runs every repository of every installation, except archived
  and disabled ones and suspended installations, plus any listed in `SWEEP_REPOSITORIES_FILE`.
* **`github.com` lookups:** on github.com runs the worker drops `GITHUB_COM_TOKEN` and `RENOVATE_GITHUB_COM_TOKEN`, so
  changelog, release-note and datasource lookups on github.com use the installation's own token and rate limit, never
  an operator's. GitLab and GHES runs still get them.
* **Rate limits:** each installation is its own credential scope, with its own run slots and cooldown (see
  [Rate limiting](rate-limiting.md)).
