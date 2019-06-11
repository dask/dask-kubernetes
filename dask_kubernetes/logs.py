class Log(str):
    """A container for logs."""

    def _widget(self):
        from ipywidgets import HTML

        return HTML(value="<pre><code>{logs}</code></pre>".format(logs=self))

    def _ipython_display_(self, **kwargs):
        return self._widget()._ipython_display_(**kwargs)


class Logs(dict):
    """A container for multiple logs."""

    def _widget(self):
        from ipywidgets import Accordion

        accordion = Accordion(children=[log._widget() for log in self.values()])
        [accordion.set_title(i, title) for i, title in enumerate(self.keys())]
        return accordion

    def _ipython_display_(self, **kwargs):
        return self._widget()._ipython_display_(**kwargs)
